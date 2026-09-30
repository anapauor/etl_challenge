# Data Engineering Challenge — Databricks ETL Pipeline

## Context

You have joined a data engineering team that receives hiring data from daily file drops and API responses.

Your task is to build a small ETL/ELT pipeline that lands the raw data, applies data quality rules, loads clean Delta tables, and creates two analytical outputs.

The challenge is intentionally small. We care more about correctness, clarity, and your ability to explain trade-offs than about the amount of code.

## Timebox

A reasonable solution should take around 4 to 6 hours of focused work.

## Preferred stack

Use the following technologies where possible:

- Python
- PySpark
- SparkSQL
- Delta Lake
- Azure Databricks notebooks or Databricks-compatible scripts

If you do not have access to Azure Databricks, provide PySpark and SQL code that would run in Databricks, and explain what would be needed to execute it there.

## What is included in this package

The package contains source data under the `data/` folder:

```text
data/
  landing/
    departments/
      departments_2021_01_01.csv
    jobs/
      jobs_2021_01_01.csv
    hired_employees/
      hired_employees_2021_01_01.csv
      hired_employees_2021_01_02.csv
  api/
    hired_employees_page_1.json
    hired_employees_page_2.json
```

The CSV files simulate daily batch files. The JSON files simulate paginated API responses. You do not need to build or expose a real API.

The source data intentionally includes a small number of data quality issues. Do not assume the inputs are clean. Your pipeline should make invalid records traceable without silently dropping them.

## Source schemas

### departments

| Column | Type | Description |
|---|---|---|
| id | integer | Department id |
| department | string | Department name |

### jobs

| Column | Type | Description |
|---|---|---|
| id | integer | Job id |
| job | string | Job name |

### hired_employees

| Column | Type | Description |
|---|---|---|
| id | integer | Employee id |
| name | string | Employee full name |
| datetime | string | Hiring datetime in ISO format |
| department_id | integer | Department id |
| job_id | integer | Job id |

## Required work

### 1. Extract to Bronze

Create a process to ingest the source data into Bronze Delta tables.

Expected Bronze tables:

```text
bronze.departments_raw
bronze.jobs_raw
bronze.hired_employees_raw
```

Requirements:

- Read CSV batch files.
- Read JSON API-like files.
- Use explicit schemas where possible.
- Add technical metadata columns:
  - `source_system`
  - `source_file`
  - `ingestion_timestamp`
  - `ingestion_date`
  - `batch_id`

### 2. Transform to Silver

Create clean Silver Delta tables:

```text
silver.departments
silver.jobs
silver.hired_employees
```

Apply these rules:

Common rules:

- Required fields must not be null.
- IDs must be valid integers.
- Duplicate records must be handled in a deterministic way.

Rules for `hired_employees`:

- `id` must not be null.
- `name` must not be null or empty.
- `datetime` must be a valid ISO timestamp.
- `department_id` must not be null and must exist in `silver.departments`.
- `job_id` must not be null and must exist in `silver.jobs`.
- If duplicate employee IDs are received, keep the latest record based on source file date or ingestion timestamp. Document your tie-breaker.

Records that are not loaded into the final Silver table must be traceable. Use this table or an equivalent audit table:

```text
silver.rejected_records
```

Suggested columns:

```text
table_name
record_id
source_system
source_file
raw_payload
rejection_reason
rejection_timestamp
batch_id
```

### 3. Load strategy

The pipeline must be idempotent.

Running the same pipeline more than once with the same input data must not create duplicated records.

For `silver.hired_employees`, use one of these strategies:

- `MERGE INTO`
- Append with deduplication
- Controlled overwrite
- Another strategy, clearly explained in the README

If you choose not to use `MERGE INTO`, explain the trade-off.

### 4. Gold layer

Create the following Gold tables or views using SparkSQL:

```text
gold.hires_by_department_job_quarter_2021
gold.departments_above_average_hiring_2021
```

#### Metric 1

Number of employees hired for each department and job in 2021, divided by quarter.

Expected output structure:

```text
department | job | Q1 | Q2 | Q3 | Q4
```

Order the result by:

```text
department ASC, job ASC
```

#### Metric 2

Departments that hired more employees than the average number of employees hired by department in 2021.

Expected output structure:

```text
id | department | hired
```

Order the result by:

```text
hired DESC
```

### 5. Validation and reconciliation

Include SQL validation queries that show the pipeline is working.

At minimum, include counts for Bronze, Silver, rejected or discarded records, and Gold outputs.

Examples:

```sql
SELECT COUNT(*) FROM bronze.hired_employees_raw;
SELECT COUNT(*) FROM silver.hired_employees;
SELECT COUNT(*) FROM silver.rejected_records;
```

Include a reconciliation check that explains how raw records flow into valid, rejected, or discarded records.

Also include a duplicate check:

```sql
SELECT id, COUNT(*)
FROM silver.hired_employees
GROUP BY id
HAVING COUNT(*) > 1;
```

### 6. Delta audit

Include Delta Lake audit queries such as:

```sql
DESCRIBE HISTORY silver.hired_employees;
```

```sql
SELECT COUNT(*)
FROM silver.hired_employees VERSION AS OF 0;
```

Optional:

```sql
SELECT *
FROM table_changes('silver.hired_employees', 0);
```

Change Data Feed is optional.

### 7. CI/CD

Include a basic `azure-pipelines.yml` file.

It does not need to deploy to a real environment, but it should show how you would structure CI/CD.

Expected steps:

- Install dependencies.
- Run unit tests.
- Run linting or formatting checks.
- Include a placeholder or example step for deploying notebooks or jobs to Databricks.

### 8. AI usage

You may use AI tools.

Include a file called:

```text
AI_USAGE.md
```

Include:

- Which AI tools you used.
- Which parts of the solution were assisted by AI.
- What you manually validated.
- What AI suggestions you rejected or corrected.
- What risks you see when using AI for data engineering work.

We will ask about this during the technical interview.

## Expected submission

Submit a Git repository or a ZIP file with your solution.

Use the project structure that you consider most appropriate. We are interested in seeing how you organize a small data engineering solution, so do not force your code into a specific folder layout just because of this document.

At minimum, your submission should include:

- A `README.md` with execution instructions and design notes.
- An `AI_USAGE.md` file, even if you did not use AI.
- The implementation of the ingestion, transformation, load, Gold outputs and validation steps.
- SQL queries or notebooks/scripts that allow us to review the analytical outputs and reconciliation checks.
- Any tests, assertions or validation checks you consider useful.
- A basic CI/CD definition or proposal. If you use Azure DevOps, an `azure-pipelines.yml` file is preferred.

The solution must be easy to review and run. Clear organization, simple naming and maintainability will be part of the evaluation.

## README expectations

Your README should explain:

- How to run the solution.
- Main design decisions.
- Assumptions.
- Data quality rules.
- Load strategy.
- How idempotency is handled.
- How rejected or discarded records are handled.
- How this would run in Azure Databricks.
- What you would improve before moving this to production.

## What we will review

We will review:

- Python code quality.
- PySpark transformations.
- SparkSQL correctness.
- Delta Lake and Lakehouse understanding.
- Data quality handling.
- Idempotent loading.
- Validation and reconciliation queries.
- CI/CD understanding.
- Documentation clarity.
- Responsible use of AI.

Do not spend time building a real API, dashboard, or full production infrastructure. Keep the scope focused on the data pipeline.
