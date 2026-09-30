"""Explicit nullable schemas for Bronze CSV reads."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DEPARTMENTS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("department", StringType(), True),
    ]
)

JOBS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("job", StringType(), True),
    ]
)

HIRED_EMPLOYEES_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("datetime", StringType(), True),
        StructField("department_id", IntegerType(), True),
        StructField("job_id", IntegerType(), True),
    ]
)

REJECTED_RECORDS_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), True),
        StructField("record_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("rejection_reason", StringType(), True),
        StructField("rejection_timestamp", TimestampType(), True),
        StructField("batch_id", StringType(), True),
    ]
)

HIRED_EMPLOYEES_API_ENVELOPE_SCHEMA = StructType(
    [
        StructField("page", IntegerType(), True),
        StructField("has_next", BooleanType(), True),
        StructField("source_system", StringType(), True),
        StructField("data", ArrayType(HIRED_EMPLOYEES_SCHEMA), True),
    ]
)
