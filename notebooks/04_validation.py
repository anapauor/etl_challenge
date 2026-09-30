# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Validation and Delta audit
# MAGIC Runs shared SQL from `sql/validation.sql`, `reconciliation.sql`, and `delta_audit.sql`.

# COMMAND ----------

from src.config import PROJECT_ROOT
from src.ingest_bronze import ensure_table_registered
from src.load_gold import _sql_statements
from src.spark_session import get_spark

spark = get_spark()

for table in (
    "bronze.hired_employees_raw",
    "silver.hired_employees",
    "silver.rejected_records",
    "gold.hires_by_department_job_quarter_2021",
    "gold.departments_above_average_hiring_2021",
):
    ensure_table_registered(spark, table)

# COMMAND ----------

print("=== validation ===")
for statement in _sql_statements(PROJECT_ROOT / "sql" / "validation.sql"):
    spark.sql(statement).show(truncate=False)

print("=== reconciliation ===")
for statement in _sql_statements(PROJECT_ROOT / "sql" / "reconciliation.sql"):
    spark.sql(statement).show(truncate=False)

print("=== delta audit ===")
for statement in _sql_statements(PROJECT_ROOT / "sql" / "delta_audit.sql"):
    spark.sql(statement).show(truncate=False)
