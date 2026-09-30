# AI usage

## Tools used

- **Grok (xAI)** in an interactive coding agent session: drafted Python/PySpark modules, SparkSQL, tests, CI YAML, notebooks, and documentation against `CHALLENGE.md` and the implementation plan.
- **Local verification only** for Spark/Delta behavior: OpenJDK 17, Python 3.11 venv, `pyspark==3.5.3`, `delta-spark==3.2.1`, `pytest`, `ruff`.

No other generative AI tools were used for this submission.

## What AI assisted

- Project skeleton (`.gitignore`, `requirements.txt`, package layout).
- `src/` modules: config, Spark session, schemas, Bronze ingest, Silver transforms, Gold loader, pipeline CLI, quality helpers.
- SQL under `sql/` (Gold metrics, validation, reconciliation, Delta audit).
- Unit tests for quality reasons and the `id=1008` tie-break.
- Thin Databricks-style notebooks under `notebooks/`.
- `azure-pipelines.yml`, `README.md`, and this file.
- Iterative fixes when local Delta/catalog edge cases appeared (session-local metastore vs warehouse paths).

## What was validated manually

These were executed and inspected on the local machine, not accepted from model output alone:

| Check | Result |
|---|---|
| Official ZIP data present (6 files) | Pass |
| Bronze hired count | **31** (`batch_csv=23`, `api_json=8`) |
| Same-batch Bronze re-run | stays **31** |
| Silver hired count | **20** |
| `id=1008` Silver name | **Daniel Wilson Updated** |
| Hired rejected rows | **11** (10 quality + 1 `duplicate_id_loser`) |
| Identity | **31 = 20 + 11** |
| Silver duplicate `id` query | **0 rows** |
| Gold metric 1 `SUM(Q1+Q2+Q3+Q4)` | **20** |
| Gold metric 2 | Supply Chain & Staff above avg **4.0** |
| `DESCRIBE HISTORY silver.hired_employees` | ≥1 version; `VERSION AS OF 0` has 20 rows |
| `ruff check src tests` | exit 0 |
| `pytest -q` | 10 passed |
| `python -m src.run_pipeline --batch-id demo` twice | idempotent Silver/rejects |

Spot-checked dirty ids in Bronze/typed flags: `1017–1022`, `2005–2008`, plus FK cases `1019` / `1020`.

## What AI suggested that was corrected or rejected

- **`CREATE OR REPLACE TABLE AS SELECT`** as written for Databricks failed on local Delta OSS → changed to `DROP` + `CREATE TABLE … USING DELTA AS`, plus path purge for external table leftovers.
- **`spark.catalog.tableExists(db, table)` two-arg form** did not reliably see managed tables → switched to qualified names and `ensure_table_registered()` from warehouse Delta paths.
- Initial risk of treating CSV blanks as FK failures → explicit rule: blanks/`null` → `null_department_id` / `null_job_id`; only real missing integers → `fk_*`.
- Avoided non-deterministic **`dropDuplicates()`** without ordering; used explicit `row_number` + documented tie-break instead.
- Did **not** invent synthetic source rows; used only the official `data/` files from the challenge package.
- Did **not** copy JSON envelope `source_system: mock_api` into the technical `source_system` column (`api_json` is set in code).
- MERGE is applied to **winners only** after in-batch dedup (never both `1008` rows as merge source).

## Risks when using AI for data engineering

1. **Silent data loss**: models often suggest `drop`, `inner` joins, or `dropDuplicates()` without proving row identity. Always reconcile Bronze → Silver → rejected with SQL counts.
2. **Wrong DQ semantics**: blank CSV vs JSON null vs missing FK are easy to conflate; tests must pin the exact rejection codes.
3. **Invented paths / schemas / counts**: never trust an example matrix or file list until it matches the ZIP on disk.
4. **Non-deterministic dedup**: unordered `dropDuplicates` is a common AI default and fails interviews and production.
5. **Platform drift**: SQL that works on Databricks may fail on OSS Delta (catalog, `CREATE OR REPLACE`, managed vs external locations).
6. **Idempotency theater**: a single successful run is not enough; re-run with the same `batch_id` and assert stable counts.
7. **Overconfident docs**: README numbers must come from executed queries, not from the prompt’s hoped-for totals.

## Bottom line

AI accelerated boilerplate and first drafts. Correctness came from executing Spark/Delta locally, asserting contract counts (`31/20/11`, `1008`, `SUM(Q)=20`), and fixing every failure the first draft hit on the real runtime.
