# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver transform
# MAGIC Thin wrapper over `src.transform_silver`. MERGE lives in shared code.

# COMMAND ----------

from src.spark_session import get_spark
from src.transform_silver import transform_dimensions, transform_hired_employees

spark = get_spark()
batch_id = "demo"

# COMMAND ----------

transform_dimensions(spark, batch_id)
transform_hired_employees(spark, batch_id)

spark.sql("SELECT COUNT(*) AS silver_employees FROM silver.hired_employees").show()
spark.sql(
    "SELECT COUNT(*) AS rejected FROM silver.rejected_records WHERE table_name = 'hired_employees'"
).show()
