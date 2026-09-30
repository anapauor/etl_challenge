# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze ingest
# MAGIC Thin wrapper over `src.ingest_bronze`. Does not reimplement writes.

# COMMAND ----------

from src.ingest_bronze import ingest_dimensions, ingest_hired_employees
from src.spark_session import get_spark

spark = get_spark()
batch_id = "demo"

# COMMAND ----------

ingest_dimensions(spark, batch_id)
ingest_hired_employees(spark, batch_id)

spark.sql(
    "SELECT source_system, COUNT(*) AS n FROM bronze.hired_employees_raw GROUP BY source_system"
).show()
