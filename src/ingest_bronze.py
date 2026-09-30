"""Bronze ingest: land CSV sources as Delta tables."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.types import StructType

from delta.tables import DeltaTable

from src.config import BATCH_ID, DATA_ROOT, WAREHOUSE_DIR
from src.schemas import (
    DEPARTMENTS_SCHEMA,
    HIRED_EMPLOYEES_API_ENVELOPE_SCHEMA,
    HIRED_EMPLOYEES_SCHEMA,
    JOBS_SCHEMA,
)

SOURCE_BATCH_CSV = "batch_csv"
SOURCE_API_JSON = "api_json"
HIRED_EMPLOYEES_RAW_TABLE = "bronze.hired_employees_raw"


def _landing_path(entity: str) -> str:
    return str(DATA_ROOT / "landing" / entity)


def source_file_col() -> Column:
    return F.element_at(F.split(F.input_file_name(), "/"), -1)


def hired_employees_file_date() -> Column:
    """Parse YYYY-MM-DD from hired_employees_YYYY_MM_DD.csv; null if unmatched."""
    return F.to_date(
        F.regexp_extract(source_file_col(), r"hired_employees_(\d{4}_\d{2}_\d{2})", 1),
        "yyyy_MM_dd",
    )


def read_csv_with_schema(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return spark.read.schema(schema).option("header", "true").csv(path)


def add_bronze_metadata(
    df: DataFrame,
    *,
    source_system: str,
    batch_id: str,
    source_file_date: Column | None = None,
    source_file: Column | None = None,
) -> DataFrame:
    file_name = source_file if source_file is not None else source_file_col()
    file_date = source_file_date if source_file_date is not None else F.lit(None).cast("date")
    return (
        df.withColumn("source_system", F.lit(source_system))
        .withColumn("source_file", file_name)
        .withColumn("source_file_date", file_date)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("batch_id", F.lit(batch_id))
    )


def table_location(qualified_name: str) -> str:
    database, _, table = qualified_name.partition(".")
    return str((WAREHOUSE_DIR / f"{database}.db" / table).resolve())


def ensure_table_registered(spark: SparkSession, qualified_name: str) -> bool:
    """Register a warehouse Delta path in this session. True if the table exists."""
    if spark.catalog.tableExists(qualified_name):
        return True
    path = table_location(qualified_name)
    if DeltaTable.isDeltaTable(spark, path):
        spark.sql(f"CREATE TABLE {qualified_name} USING DELTA LOCATION '{path}'")
        return True
    return False


def overwrite_delta(df: DataFrame, table: str) -> None:
    ensure_table_registered(df.sparkSession, table)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def delete_batch_and_append(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    batch_id: str,
) -> None:
    if ensure_table_registered(spark, table):
        escaped = batch_id.replace("'", "''")
        spark.sql(f"DELETE FROM {table} WHERE batch_id = '{escaped}'")
    df.write.format("delta").mode("append").saveAsTable(table)


def ingest_departments(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    df = add_bronze_metadata(
        read_csv_with_schema(spark, _landing_path("departments"), DEPARTMENTS_SCHEMA),
        source_system=SOURCE_BATCH_CSV,
        batch_id=batch_id or BATCH_ID,
    )
    overwrite_delta(df, "bronze.departments_raw")
    return df


def ingest_jobs(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    df = add_bronze_metadata(
        read_csv_with_schema(spark, _landing_path("jobs"), JOBS_SCHEMA),
        source_system=SOURCE_BATCH_CSV,
        batch_id=batch_id or BATCH_ID,
    )
    overwrite_delta(df, "bronze.jobs_raw")
    return df


def ingest_dimensions(spark: SparkSession, batch_id: str | None = None) -> None:
    ingest_departments(spark, batch_id)
    ingest_jobs(spark, batch_id)


def ingest_hired_employees_csv(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    """Read landing CSVs into bronze_tmp_csv. Does not write hired_employees_raw."""
    df = add_bronze_metadata(
        read_csv_with_schema(spark, _landing_path("hired_employees"), HIRED_EMPLOYEES_SCHEMA),
        source_system=SOURCE_BATCH_CSV,
        batch_id=batch_id or BATCH_ID,
        source_file_date=hired_employees_file_date(),
    )
    df.createOrReplaceTempView("bronze_tmp_csv")
    return df


def ingest_hired_employees_json(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    """Read paginated API JSON into bronze_tmp_json. Does not write hired_employees_raw."""
    raw = (
        spark.read.option("multiLine", "true")
        .schema(HIRED_EMPLOYEES_API_ENVELOPE_SCHEMA)
        .json(str(DATA_ROOT / "api"))
        .withColumn("_source_file", source_file_col())
    )
    exploded = raw.select(F.col("_source_file"), F.explode("data").alias("emp")).select(
        "emp.*",
        "_source_file",
    )
    df = add_bronze_metadata(
        exploded,
        source_system=SOURCE_API_JSON,
        batch_id=batch_id or BATCH_ID,
        source_file=F.col("_source_file"),
    ).drop("_source_file")
    df.createOrReplaceTempView("bronze_tmp_json")
    return df


def ingest_hired_employees(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    """Union CSV + JSON and write bronze.hired_employees_raw idempotently by batch_id."""
    bid = batch_id or BATCH_ID
    unioned = ingest_hired_employees_csv(spark, bid).unionByName(
        ingest_hired_employees_json(spark, bid)
    )
    delete_batch_and_append(spark, unioned, HIRED_EMPLOYEES_RAW_TABLE, bid)
    return unioned
