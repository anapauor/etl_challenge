# Implementation backlog — Hiring ETL

Bronze → Silver → Gold pipeline. Local Spark + Delta. No Azure required.

How to use this document:

- Implement items **in order**. Do not skip.
- Each item ends as **Done** or **Blocked**.
- If a check fails, stop.
- Official data: `databricks_etl_challenge_sent.zip`. Do not invent or “fix” rows.

---

## Contracts (read before coding)

### Paths

```text
DATA_ROOT = data                    # folder copied from the ZIP
CSV  = {DATA_ROOT}/landing/{entity}/*.csv
JSON = {DATA_ROOT}/api/*.json
WAREHOUSE = ./spark-warehouse
```

The challenge text draws `data/landing/api/`. The real ZIP has `data/api/`. Follow the ZIP.

### Tables (exact names)

```text
bronze.departments_raw
bronze.jobs_raw
bronze.hired_employees_raw

silver.departments
silver.jobs
silver.hired_employees
silver.rejected_records

gold.hires_by_department_job_quarter_2021
gold.departments_above_average_hiring_2021
```

### Business schemas

```text
departments:      id INT, department STRING
jobs:             id INT, job STRING
hired_employees:  id INT, name STRING, datetime STRING, department_id INT, job_id INT
```

### Bronze technical metadata (every raw table)

```text
source_system         STRING    batch_csv | api_json
source_file           STRING    file name
source_file_date      DATE      YYYY-MM-DD parsed from hired_employees_YYYY_MM_DD.csv; null for API and dims
ingestion_timestamp   TIMESTAMP current_timestamp()
ingestion_date        DATE      current_date()
batch_id              STRING    CLI / env BATCH_ID
```

Do **not** copy the JSON field `source_system: mock_api` into the technical column. That column is always `batch_csv` or `api_json`.

### Silver hired_employees

```text
id INT
name STRING
hired_at TIMESTAMP
department_id INT
job_id INT
source_system STRING
source_file STRING
source_file_date DATE
ingestion_timestamp TIMESTAMP
batch_id STRING
```

### silver.rejected_records

```text
table_name            STRING
record_id             STRING
source_system         STRING
source_file           STRING
raw_payload           STRING     JSON of the raw row
rejection_reason      STRING     reasons joined with |
rejection_timestamp   TIMESTAMP
batch_id              STRING
```

### Rejection reasons

| Code | When |
|---|---|
| `null_id` | id is null or not an integer |
| `null_or_empty_name` | name is null or blank after trim |
| `invalid_datetime` | value does not parse as ISO |
| `null_department_id` | department_id is null, not an integer, **or a CSV empty string** |
| `null_job_id` | job_id is null, not an integer, **or a CSV empty string** |
| `fk_department` | integer id that does not exist in silver.departments |
| `fk_job` | integer id that does not exist in silver.jobs |
| `duplicate_id_loser` | lost the employee `id` tie-break |

CSV empty strings (`1021`, `1022`) and JSON `null` (`2005`, `2007`, `2008`) are the same class of rejection. Do not label blanks as `fk_*`.

### Datetime

Accepted formats, in this order:

1. `yyyy-MM-dd'T'HH:mm:ssX`  (the dataset uses `...Z`)
2. `yyyy-MM-dd'T'HH:mm:ss`
3. `yyyy-MM-dd HH:mm:ss`

Anything else (`invalid_date`, `bad_timestamp`) → `invalid_datetime`.

### Employee `id` tie-break

Winner is the first rule that breaks the tie:

1. Latest `source_file_date` (null loses to a real date)
2. Latest `ingestion_timestamp`
3. Lexicographically greater `source_file`
4. Single remaining row (`row_number = 1`)

Real case: `id=1008` from `hired_employees_2021_01_02.csv` wins. Expected name: `Daniel Wilson Updated`.

### Silver employee load order (mandatory)

1. Type + quality flags (CSV blank vs JSON null).
2. FK join to published dims.
3. Build `rejection_reason`.
4. Split valid vs quality-rejected.
5. `row_number` on valids → winner / `duplicate_id_loser`.
6. `MERGE` **winners only** into Silver on `id`.

Never MERGE both `1008` rows without the window first.

### Idempotency

- Dim Bronze/Silver: `overwrite` (small snapshots).
- Fact Bronze: `DELETE WHERE batch_id = current` + append.
- Rejected for the batch: `DELETE WHERE batch_id = current AND table_name = ...` + append.
- Silver employees: `MERGE INTO` on `id` (update only if the source is newer per the tie-break).

### First-run counts (empty warehouse)

| Table | n |
|---|---|
| bronze.departments_raw | 5 |
| bronze.jobs_raw | 5 |
| bronze.hired_employees_raw | 31 |
| silver.departments | 5 |
| silver.jobs | 5 |
| silver.hired_employees | 20 |
| silver.rejected_records (hired_employees) | 11 |

Identity: `31 = 20 + 11`.

Silver IDs (20):

`1001–1007, 1008, 1009–1016, 2001–2004`

Quality-rejected IDs (10):

`1017, 1018, 1019, 1020, 1021, 1022, 2005, 2006, 2007, 2008`

Duplicate loser (1): `1008` from `hired_employees_2021_01_01.csv`

JSON API envelope: `{ page, has_next, source_system, data: [...] }`. Explode `data`.

Official package layout:

```text
databricks_etl_challenge/
├── CHALLENGE.md
├── README.md
└── data/
    ├── api/
    │   ├── hired_employees_page_1.json
    │   └── hired_employees_page_2.json
    └── landing/
        ├── departments/departments_2021_01_01.csv
        ├── jobs/jobs_2021_01_01.csv
        └── hired_employees/
            ├── hired_employees_2021_01_01.csv
            └── hired_employees_2021_01_02.csv
```

---

## Repo layout

```text
hiring-etl/
├── README.md
├── AI_USAGE.md
├── azure-pipelines.yml
├── requirements.txt
├── .gitignore
├── data/                         # copy from the ZIP
│   ├── api/...
│   └── landing/...
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── spark_session.py
│   ├── schemas.py
│   ├── quality.py
│   ├── ingest_bronze.py
│   ├── transform_silver.py
│   ├── load_gold.py
│   └── run_pipeline.py
├── sql/
│   ├── gold_metrics.sql
│   ├── validation.sql
│   ├── reconciliation.sql
│   └── delta_audit.sql
├── tests/
│   ├── __init__.py
│   ├── test_quality.py
│   └── test_dedup.py
└── notebooks/                    # thin wrappers over src/
    ├── 01_bronze.py
    ├── 02_silver.py
    ├── 03_gold.py
    └── 04_validation.py
```

`requirements.txt`:

```text
pyspark==3.5.3
delta-spark==3.2.1
pytest==8.3.3
ruff==0.6.9
```

If the PySpark/Delta pair fails to start, pin a compatible pair and document it. Do not continue without working Delta.

---

# Backlog

Each item: **ID — title**, done criteria, check command/SQL.

Implementation status: `[ ]` → `[x]`.

---

## Epic A — Bootstrap

### A1 — Create repo and copy data

Done when the tree exists and `data/landing` + `data/api` contain the six official files.

Check:

```bash
test -f data/landing/departments/departments_2021_01_01.csv
test -f data/landing/jobs/jobs_2021_01_01.csv
test -f data/landing/hired_employees/hired_employees_2021_01_01.csv
test -f data/landing/hired_employees/hired_employees_2021_01_02.csv
test -f data/api/hired_employees_page_1.json
test -f data/api/hired_employees_page_2.json
```

- [x] A1 Done

### A2 — config + Spark + Delta + databases

`src/config.py`: `DATA_ROOT=data`, `WAREHOUSE_DIR`, `BATCH_ID`.  
`src/spark_session.py`: master `local[*]`, Delta extensions, local warehouse.  
On start: `CREATE DATABASE IF NOT EXISTS bronze|silver|gold`.

Check:

```bash
python -c "from src.spark_session import get_spark; s=get_spark(); s.sql('CREATE DATABASE IF NOT EXISTS bronze'); print([r[0] for r in s.sql('SHOW DATABASES').collect()])"
```

Pass: databases print and include `bronze`. Fail (Java / Delta / import) → stop.

- [x] A2 Done

### A3 — Explicit schemas

`src/schemas.py` with nullable `StructType` for departments, jobs, hired_employees CSV.

Check: import works; employee schema has the five business fields.

- [x] A3 Done

---

## Epic B — Bronze

### B1 — Ingest departments and jobs

Read CSV with schema. Add metadata (`source_system=batch_csv`).  
Delta `overwrite` → `bronze.departments_raw`, `bronze.jobs_raw`.

Check:

```sql
SELECT COUNT(*) FROM bronze.departments_raw;   -- 5
SELECT COUNT(*) FROM bronze.jobs_raw;          -- 5
SELECT source_system, source_file, batch_id FROM bronze.departments_raw LIMIT 1;
DESCRIBE bronze.departments_raw;
```

Re-run dim ingest: COUNT stays 5.

- [x] B1 Done

### B2 — Ingest employee CSVs

Read `data/landing/hired_employees/*.csv`.  
Parse `source_file_date` from `hired_employees_YYYY_MM_DD.csv`.  
Metadata `batch_csv`.

Intermediate check (before JSON union):

```sql
SELECT source_file, COUNT(*) FROM bronze_tmp_csv GROUP BY source_file;
-- ..._01_01.csv = 12
-- ..._01_02.csv = 11
```

- [x] B2 Done

### B3 — Ingest JSON API

`read.option("multiLine","true").json("data/api")`.  
`explode(data)` → same business columns as CSV.  
`source_system=api_json`, `source_file_date` null.

Intermediate check: 3 + 5 = 8 API rows.

- [x] B3 Done

### B4 — Union + idempotent write of `bronze.hired_employees_raw`

Union by column name.  
`DELETE FROM bronze.hired_employees_raw WHERE batch_id = :batch` (if table exists) + append.  
Create the table if missing.

Check:

```sql
SELECT source_system, COUNT(*) FROM bronze.hired_employees_raw GROUP BY source_system;
-- batch_csv = 23
-- api_json  = 8
SELECT COUNT(*) FROM bronze.hired_employees_raw;  -- 31
```

Run B4 twice with the same `BATCH_ID`. COUNT stays 31.

- [x] B4 Done

---

## Epic C — Testable quality

### C1 — Pure helpers in `quality.py`

Python functions (no Spark) at minimum:

- `is_blank(value) -> bool`
- `parse_int(value) -> int | None`
- `parse_iso(value) -> datetime | None`
- `quality_reasons(row_dict, dept_ids: set, job_ids: set) -> list[str]`

Check: `from src.quality import quality_reasons`.

- [x] C1 Done

### C2 — Unit tests

`tests/test_quality.py`:

| Case | Expected |
|---|---|
| name `"   "` | `null_or_empty_name` |
| name `None` | `null_or_empty_name` |
| datetime `"invalid_date"` | `invalid_datetime` |
| datetime `"bad_timestamp"` | `invalid_datetime` |
| datetime `"2021-01-15T10:00:00Z"` | no datetime reason |
| department_id `""` | `null_department_id` |
| department_id `None` | `null_department_id` |
| department_id `99` with dims `{1..5}` | `fk_department` |
| blank name + bad datetime | both reasons |

`tests/test_dedup.py`: two dicts with `id=1008`, file dates 01 vs 02 → 02 wins.

Check: `pytest -q` green.

- [x] C2 Done

---

## Epic D — Silver dimensions

### D1 — Clean departments and jobs

Cast id. Reject null/blank id or name (this ZIP has no dirty dims; still handle it).  
Dedup by id.  
Overwrite `silver.departments`, `silver.jobs`.  
Invalids → `rejected_records` with the matching `table_name`.

Check:

```sql
SELECT COUNT(*) FROM silver.departments;  -- 5
SELECT COUNT(*) FROM silver.jobs;         -- 5
SELECT id FROM silver.departments ORDER BY id;
-- 1,2,3,4,5
SELECT id, COUNT(*) FROM silver.departments GROUP BY id HAVING COUNT(*) > 1;
-- 0 rows
```

- [x] D1 Done

---

## Epic E — Silver employees

### E1 — Typed DF + flags (no FK yet)

From Bronze. Treat `""` as null for ids and name. Parse `hired_at`.

Spot check:

```sql
SELECT id, name, department_id, job_id, datetime
FROM bronze.hired_employees_raw
WHERE id IN (1017, 1018, 1021, 1022, 2005, 2006, 2007, 2008)
ORDER BY id;
```

All 8 exist in raw. Flags must mark them.

- [x] E1 Done

### E2 — FK + quality split

Join `silver.departments` / `silver.jobs`.  
`1019` → `fk_department`. `1020` → `fk_job`.  
Split: empty reason = valid candidate.

Check: valid candidates = 21 rows (20 winners + the day-01 `1008` duplicate).  
Quality rejected = 10.

- [x] E2 Done

### E3 — Dedup + loser rejection

`row_number` with the tie-break.  
`rn=1` → winners (20).  
`rn>1` → rejected `duplicate_id_loser` (1).

Check:

```text
winners.id includes 1008
winners.name for 1008 = "Daniel Wilson Updated"
winners.count = 20
losers.count = 1
losers.source_file contains hired_employees_2021_01_01.csv
```

- [x] E3 Done

### E4 — MERGE + persist rejected

MERGE winners into `silver.hired_employees` on `id`.  
Delete current-batch rejected rows for `hired_employees` + append (quality + losers).

Check:

```sql
SELECT COUNT(*) FROM silver.hired_employees;           -- 20
SELECT name FROM silver.hired_employees WHERE id=1008; -- Daniel Wilson Updated
SELECT id, COUNT(*) FROM silver.hired_employees GROUP BY id HAVING COUNT(*) > 1;
-- 0 rows
SELECT record_id, rejection_reason
FROM silver.rejected_records
WHERE table_name='hired_employees'
ORDER BY record_id;
```

Re-run E4 with the same `BATCH_ID`: silver COUNT=20, hired rejected=11.

- [x] E4 Done

---

## Epic F — Gold (SparkSQL)

Put SQL in `sql/gold_metrics.sql` and run it from `load_gold.py`.  
Filter: `year(hired_at) = 2021` (all 20 are).

### F1 — Metric 1

```sql
CREATE OR REPLACE TABLE gold.hires_by_department_job_quarter_2021 AS
SELECT
  d.department,
  j.job,
  SUM(CASE WHEN quarter(e.hired_at) = 1 THEN 1 ELSE 0 END) AS Q1,
  SUM(CASE WHEN quarter(e.hired_at) = 2 THEN 1 ELSE 0 END) AS Q2,
  SUM(CASE WHEN quarter(e.hired_at) = 3 THEN 1 ELSE 0 END) AS Q3,
  SUM(CASE WHEN quarter(e.hired_at) = 4 THEN 1 ELSE 0 END) AS Q4
FROM silver.hired_employees e
JOIN silver.departments d ON e.department_id = d.id
JOIN silver.jobs j ON e.job_id = j.id
WHERE year(e.hired_at) = 2021
GROUP BY d.department, j.job
ORDER BY d.department ASC, j.job ASC;
```

Check:

```sql
DESCRIBE gold.hires_by_department_job_quarter_2021;
-- columns: department, job, Q1, Q2, Q3, Q4

SELECT SUM(Q1+Q2+Q3+Q4) FROM gold.hires_by_department_job_quarter_2021;
-- 20

SELECT * FROM gold.hires_by_department_job_quarter_2021;
-- visual: department ASC, job ASC
```

- [x] F1 Done

### F2 — Metric 2

```sql
CREATE OR REPLACE TABLE gold.departments_above_average_hiring_2021 AS
WITH hires AS (
  SELECT d.id, d.department, COUNT(*) AS hired
  FROM silver.hired_employees e
  JOIN silver.departments d ON e.department_id = d.id
  WHERE year(e.hired_at) = 2021
  GROUP BY d.id, d.department
),
avg_h AS (SELECT AVG(hired) AS avg_hired FROM hires)
SELECT h.id, h.department, h.hired
FROM hires h
CROSS JOIN avg_h a
WHERE h.hired > a.avg_hired
ORDER BY h.hired DESC;
```

Check:

```sql
DESCRIBE gold.departments_above_average_hiring_2021;
-- id, department, hired

SELECT * FROM gold.departments_above_average_hiring_2021;
-- hired DESC; every row > average hires per department
```

Compute the five department `hired` counts by hand in the README after you have output. Do not invent the average before running.

- [x] F2 Done

---

## Epic G — Validation, reconciliation, audit

### G1 — validation.sql

```sql
SELECT 'bronze.departments_raw' t, COUNT(*) n FROM bronze.departments_raw
UNION ALL SELECT 'bronze.jobs_raw', COUNT(*) FROM bronze.jobs_raw
UNION ALL SELECT 'bronze.hired_employees_raw', COUNT(*) FROM bronze.hired_employees_raw
UNION ALL SELECT 'silver.departments', COUNT(*) FROM silver.departments
UNION ALL SELECT 'silver.jobs', COUNT(*) FROM silver.jobs
UNION ALL SELECT 'silver.hired_employees', COUNT(*) FROM silver.hired_employees
UNION ALL SELECT 'silver.rejected_records', COUNT(*) FROM silver.rejected_records
UNION ALL SELECT 'gold.m1', COUNT(*) FROM gold.hires_by_department_job_quarter_2021
UNION ALL SELECT 'gold.m2', COUNT(*) FROM gold.departments_above_average_hiring_2021;

SELECT id, COUNT(*) c
FROM silver.hired_employees
GROUP BY id
HAVING COUNT(*) > 1;
```

Check: first query matches the contract counts. Second query returns 0 rows.

- [x] G1 Done

### G2 — reconciliation.sql

First run:

```sql
SELECT
  (SELECT COUNT(*) FROM bronze.hired_employees_raw) AS bronze_rows,
  (SELECT COUNT(*) FROM silver.hired_employees) AS silver_ids,
  (SELECT COUNT(*) FROM silver.rejected_records WHERE table_name='hired_employees') AS rejected_he,
  (SELECT COUNT(*) FROM silver.rejected_records
    WHERE table_name='hired_employees' AND rejection_reason LIKE '%duplicate_id_loser%') AS dup_losers;
```

Check: `31, 20, 11, 1`.  
Document in README: with a **new** `batch_id`, Bronze grows; Silver is no longer 1:1 with Bronze.

- [x] G2 Done

### G3 — delta_audit.sql

```sql
DESCRIBE HISTORY silver.hired_employees;
SELECT COUNT(*) FROM silver.hired_employees VERSION AS OF 0;
```

CDF optional and commented. If `VERSION AS OF 0` is an empty create, document the first version that has rows.

Check: `DESCRIBE HISTORY` runs and returns ≥1 row.

- [x] G3 Done

---

## Epic H — Orchestration

### H1 — `python -m src.run_pipeline`

CLI:

```bash
python -m src.run_pipeline --data-root data --batch-id demo
```

Internal order: Spark → DBs → B1–B4 → D1 → E1–E4 → F1–F2 → print G1.

Check:

```bash
python -m src.run_pipeline --batch-id demo
python -m src.run_pipeline --batch-id demo
```

Second run: silver=20, hired rejected=11, bronze hired=31.

- [x] H1 Done

### H2 — Notebook wrappers

Four notebooks that import `src/` functions. Do not reimplement MERGE.

Check: four files exist and call shared functions.

- [x] H2 Done

---

## Epic I — CI and docs

### I1 — ruff + pytest clean

```bash
ruff check src tests
pytest -q
```

Pass: exit 0.

- [x] I1 Done

### I2 — azure-pipelines.yml

Steps: Python 3.10 → pip → ruff → pytest → echo placeholder `databricks bundle deploy --target dev`.

Check: YAML contains those four blocks.

- [x] I2 Done

### I3 — README.md

Required sections:

1. How to run locally (Java, pip, exact command)
2. Design decisions
3. Assumptions (`DATA_ROOT`, JSON envelope, CSV blank = null)
4. Data quality rules
5. Load strategy (in-batch dedup + MERGE) and why
6. Idempotency
7. Rejected records
8. How this would run on Azure Databricks
9. Production improvements (Autoloader, Unity Catalog, Jobs)
10. Actual first-run counts

- [x] I3 Done

### I4 — AI_USAGE.md

Tools used, what AI drafted, what was validated by hand (counts, `1008`, `SUM(Q)=20`), what was corrected, risks (non-deterministic `dropDuplicates`, invented paths, swallowed FKs).

Check: both markdown files exist and are non-empty.

- [x] I4 Done

---

## Epic J — Wrap-up

### J1 — Final checklist

- [x] A1 uses ZIP data, not synthetic rows
- [x] Bronze hired = 31 after re-run
- [x] Silver hired = 20, id 1008 name updated
- [x] Rejected hired = 11
- [x] Silver duplicate check empty
- [x] Gold column names exact and `SUM(Qx)=20`
- [x] Table history works
- [x] README + AI_USAGE + YAML present
- [x] No secrets; warehouse not in git

### J2 — Package

```bash
zip -r hiring-etl.zip hiring-etl \
  -x "hiring-etl/.venv/*" \
  -x "hiring-etl/spark-warehouse/*" \
  -x "hiring-etl/**/__pycache__/*"
```

- [x] J1 Done
- [x] J2 Done

---

## Project definition of done

```bash
pip install -r requirements.txt
pytest -q
python -m src.run_pipeline --data-root data --batch-id demo
python -m src.run_pipeline --data-root data --batch-id demo
```

All green, contract counts match.

---

## Suggested schedule (4–6 hours)

| Items | Time |
|---|---|
| A1–A3 | 25 min |
| B1–B4 | 50 min |
| C1–C2 | 30 min |
| D1 | 15 min |
| E1–E4 | 75 min |
| F1–F2 | 25 min |
| G1–G3 | 25 min |
| H1–H2 | 20 min |
| I1–I4 | 30 min |
| J1–J2 | 15 min |

---

## Treat these as Blocked

1. Reading JSON from `data/landing/api`
2. Treating JSON as a root array
3. `dropDuplicates(["id"])` with no order
4. MERGE before `row_number`
5. Bronze append without delete-by-`batch_id`
6. Dropping invalids without `rejected_records`
7. Labeling CSV `""` as `fk_*`
8. Gold columns named `q1` / `quarter_1`
9. Using `mock_api` as the technical `source_system`
10. Inventing rows or “fixing” the empty name
