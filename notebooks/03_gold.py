# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold metrics
# MAGIC Thin wrapper over `src.load_gold` / `sql/gold_metrics.sql`.

# COMMAND ----------

from src.load_gold import load_gold
from src.spark_session import get_spark

spark = get_spark()

# COMMAND ----------

load_gold(spark)

spark.sql(
    "SELECT SUM(Q1 + Q2 + Q3 + Q4) AS total_hires FROM gold.hires_by_department_job_quarter_2021"
).show()
spark.sql(
    "SELECT * FROM gold.departments_above_average_hiring_2021 ORDER BY hired DESC"
).show()
