# Hiring ETL

Bronze → Silver → Gold lakehouse pipeline for the Databricks data engineering challenge. Official source files live under `data/`; shared logic lives under `src/`; SparkSQL metrics and checks live under `sql/`.

## 1. How to run locally

### Prerequisites

- **Java 17** (Spark driver). On macOS with Homebrew: `brew install openjdk@17`.
- **Python 3.10+** (CI uses 3.10; local development was verified with 3.11).
- Official challenge data under `data/landing` and `data/api` (already included).

### Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`src/spark_session.py` auto-detects Homebrew OpenJDK 17 when `JAVA_HOME` is unset. You can also export it explicitly:

```bash
export JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null || echo /opt/homebrew/opt/openjdk@17)"
```

### Run the pipeline

```bash
python -m src.run_pipeline --data-root data --batch-id demo
```

Re-run with the same `--batch-id` to confirm idempotency (Silver stays at 20 hired employees; hired rejects stay at 11; Bronze hired stays at 31).

### Quality gates

```bash
ruff check src tests
pytest -q
```

### Useful SQL

After a successful run (same Spark warehouse):

```bash
# Layer counts + duplicate check
# sql/validation.sql

# bronze → silver → rejected identity
# sql/reconciliation.sql

# DESCRIBE HISTORY / VERSION AS OF
# sql/delta_audit.sql
```

Notebooks under `notebooks/` are thin Databricks-style wrappers over the same `src/` functions.

## 2. Design decisions

- **Lakehouse medallion**: raw land in Bronze, cleaned published tables in Silver, analytics in Gold.
- **Explicit schemas** for CSV and the JSON API envelope (no schema inference for business columns).
- **Shared Python modules** (`src/`) as the source of truth; notebooks and CI call those modules.
- **Local Spark + Delta OSS** so the solution runs without a Databricks workspace, while staying Databricks-compatible.
- **Technical metadata** on every Bronze row: `source_system`, `source_file`, `source_file_date`, `ingestion_timestamp`, `ingestion_date`, `batch_id`.
- **Rejected records are auditable**: invalid or losing rows go to `silver.rejected_records` instead of disappearing.

## 3. Assumptions

- `DATA_ROOT` points at a folder with `landing/` and `api/` (default: `./data`). Override with `--data-root` or env `DATA_ROOT`.
- JSON files are **paginated envelopes** `{ page, has_next, source_system, data: [...] }`. Business rows come from `explode(data)`. The payload field `source_system: mock_api` is **not** copied into the technical `source_system` column (Bronze uses `api_json`).
- CSV empty strings for integer/name fields are treated as **null** (same rejection class as JSON `null`).
- Department and job files are small full snapshots; hired employees are facts keyed by `id` with possible duplicates across files/sources.
- Local Spark uses an in-memory catalog per process. Delta table files under `spark-warehouse/` are re-registered when needed so reruns across processes still find existing tables.

## 4. Data quality rules

Common:

- Required fields must not be null / blank after trim.
- IDs must be valid integers.
- Duplicates are resolved deterministically (see tie-break below).

`hired_employees` reasons (joined with `|` when multiple apply):

| Code | When |
|---|---|
| `null_id` | id null / not an integer |
| `null_or_empty_name` | name null or blank |
| `invalid_datetime` | not one of the accepted ISO formats |
| `null_department_id` | null, non-integer, or CSV blank |
| `null_job_id` | null, non-integer, or CSV blank |
| `fk_department` | integer department_id missing from `silver.departments` |
| `fk_job` | integer job_id missing from `silver.jobs` |
| `duplicate_id_loser` | lost the in-batch `id` tie-break |

Blanks are never labeled as `fk_*`.

Accepted datetime formats (in order):

1. `yyyy-MM-dd'T'HH:mm:ssX` (dataset uses `...Z`)
2. `yyyy-MM-dd'T'HH:mm:ss`
3. `yyyy-MM-dd HH:mm:ss`

Employee `id` tie-break (winner = first rule that decides):

1. Latest `source_file_date` (null loses to a real date)
2. Latest `ingestion_timestamp`
3. Lexicographically greater `source_file`

Real case: `id=1008` from `hired_employees_2021_01_02.csv` wins → name `Daniel Wilson Updated`.

## 5. Load strategy (in-batch dedup + MERGE) and why

Silver employee load order:

1. Type + quality flags (CSV blank vs JSON null).
2. FK join to published Silver dims.
3. Build `rejection_reason`.
4. Split valid vs quality-rejected.
5. `row_number` on valids → winners / `duplicate_id_loser`.
6. **MERGE winners only** into `silver.hired_employees` on `id`.

Why MERGE (instead of append-only or blind overwrite):

- Facts are keyed by employee `id` and can arrive again in later batches with updates.
- MERGE upserts on `id` and updates only when the source row is **newer** by the same tie-break used in dedup.
- In-batch dedup runs **before** MERGE so both `1008` rows never enter the merge source together.

Dimensions use overwrite (small snapshots). That is simpler and correct for full daily dim drops.

Trade-off vs append+dedup views: MERGE keeps a single current row per `id` in the published Silver table, which matches how Gold metrics expect unique employees.

## 6. Idempotency

| Layer | Strategy |
|---|---|
| Bronze dims | `overwrite` |
| Bronze hired employees | `DELETE WHERE batch_id = current` + append |
| Silver dims | `overwrite` |
| Silver hired employees | `MERGE INTO` on `id` (update if source newer) |
| Rejected records | `DELETE WHERE batch_id = current AND table_name = ...` + append |
| Gold | drop/recreate Delta tables from SparkSQL |

Same `--batch-id` + same inputs → same Silver/Gold counts (no duplicated published employees).

A **new** `batch_id` with the same files appends another Bronze hired batch (31 more raw rows). Silver stays unique via MERGE; reconciliation is no longer a simple `bronze = silver + rejected` for the whole table history.

## 7. Rejected records

Table: `silver.rejected_records`

Columns: `table_name`, `record_id`, `source_system`, `source_file`, `raw_payload`, `rejection_reason`, `rejection_timestamp`, `batch_id`.

First-run hired rejects (11):

- Quality (10): `1017, 1018, 1019, 1020, 1021, 1022, 2005, 2006, 2007, 2008`
- Duplicate loser (1): `1008` from `hired_employees_2021_01_01.csv`

Identity for a single batch: `31 = 20 + 11`.

## 8. How this would run on Azure Databricks

1. Create a Unity Catalog (or Hive metastore) schema trio: `bronze`, `silver`, `gold`.
2. Upload / Autoload files into a volume or ADLS path mapped as `DATA_ROOT`.
3. Install the same Python deps on the cluster (`pyspark` is provided; add `delta-spark` only if not already on the runtime).
4. Deploy notebooks from `notebooks/` or run `python -m src.run_pipeline` as a Databricks Job task.
5. Point `WAREHOUSE_DIR` / table locations at UC managed storage instead of local `spark-warehouse/`.
6. Wire `azure-pipelines.yml` deploy step to `databricks bundle deploy --target dev` (placeholder already present).
7. Use Job parameters for `--batch-id` and `--data-root`.

Local `get_spark()` is a stand-in for the Databricks-provided `spark` session; notebooks already call the shared modules the same way.

## 9. Production improvements

- **Autoloader** (cloudFiles) for incremental landing instead of full directory reads.
- **Unity Catalog** for governance, grants, and lineage.
- **Databricks Workflows / Jobs** with retries, SLA alerts, and task dependencies (dims before facts).
- **Expectations / DLT** or centralized DQ service for rule management.
- **Change Data Feed** on Silver for downstream incremental Gold.
- **Persistent metastore** (or UC) so table registration is not session-local.
- **Structured logging + metrics** (row counts, reject rates per reason) to a monitoring sink.
- **Partitioning / Z-ORDER** on `hired_at` / `department_id` if volumes grow.
- **Secrets** via Databricks secrets / Key Vault (no credentials in repo).

## 10. Actual first-run counts

Observed after `python -m src.run_pipeline --data-root data --batch-id demo` on an empty warehouse (and stable on same-batch rerun):

| Table | n |
|---|---|
| `bronze.departments_raw` | 5 |
| `bronze.jobs_raw` | 5 |
| `bronze.hired_employees_raw` | 31 |
| `silver.departments` | 5 |
| `silver.jobs` | 5 |
| `silver.hired_employees` | 20 |
| `silver.rejected_records` (hired_employees) | 11 |
| `gold.hires_by_department_job_quarter_2021` | 14 rows; `SUM(Q1+Q2+Q3+Q4) = 20` |
| `gold.departments_above_average_hiring_2021` | 2 rows (avg hired/department = 4.0; Supply Chain & Staff at 5) |

Silver employee ids (20): `1001–1007, 1008, 1009–1016, 2001–2004`.

Department hired totals used for metric 2: Supply Chain 5, Staff 5, IT 4, Finance 3, Maintenance 3.
