"""Silver transforms: clean dimensions and persist rejected records."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import BATCH_ID
from src.ingest_bronze import ensure_table_registered, overwrite_delta
from src.schemas import REJECTED_RECORDS_SCHEMA

REJECTED_TABLE = "silver.rejected_records"
SILVER_DEPARTMENTS = "silver.departments"
SILVER_JOBS = "silver.jobs"
SILVER_HIRED_EMPLOYEES = "silver.hired_employees"

SILVER_EMPLOYEE_COLS = (
    "id",
    "name",
    "hired_at",
    "department_id",
    "job_id",
    "source_system",
    "source_file",
    "source_file_date",
    "ingestion_timestamp",
    "batch_id",
)

# Source is newer per the employee id tie-break (null source_file_date loses).
_NEWER_SOURCE = """
(s.source_file_date IS NOT NULL AND t.source_file_date IS NULL)
OR (s.source_file_date > t.source_file_date)
OR (
  s.source_file_date <=> t.source_file_date
  AND s.ingestion_timestamp > t.ingestion_timestamp
)
OR (
  s.source_file_date <=> t.source_file_date
  AND s.ingestion_timestamp <=> t.ingestion_timestamp
  AND s.source_file > t.source_file
)
"""

_HELPER_COLS = ("rejection_reason", "_rn")


def _sql_str(value: str) -> str:
    return value.replace("'", "''")


def dim_rejection_reason(name_col: str):
    return F.concat_ws(
        "|",
        F.when(F.col("id").isNull(), F.lit("null_id")),
        F.when(
            F.col(name_col).isNull() | (F.trim(F.col(name_col)) == ""),
            F.lit("null_or_empty_name"),
        ),
    )


def rank_by_id(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("id").orderBy(
        F.col("source_file_date").desc_nulls_last(),
        F.col("ingestion_timestamp").desc(),
        F.col("source_file").desc(),
    )
    return df.withColumn("_rn", F.row_number().over(window))


def to_rejected_records(df: DataFrame, table_name: str, batch_id: str) -> DataFrame:
    payload_cols = [c for c in df.columns if c not in _HELPER_COLS]
    return (
        df.withColumn("table_name", F.lit(table_name))
        .withColumn("record_id", F.col("id").cast("string"))
        .withColumn("raw_payload", F.to_json(F.struct(*[F.col(c) for c in payload_cols])))
        .withColumn("rejection_timestamp", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .select(*REJECTED_RECORDS_SCHEMA.fieldNames())
    )


def persist_rejected(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    batch_id: str,
) -> None:
    if ensure_table_registered(spark, REJECTED_TABLE):
        spark.sql(
            f"DELETE FROM {REJECTED_TABLE} "
            f"WHERE batch_id = '{_sql_str(batch_id)}' AND table_name = '{_sql_str(table_name)}'"
        )
    df.write.format("delta").mode("append").saveAsTable(REJECTED_TABLE)


def _clean_dimension(
    spark: SparkSession,
    *,
    bronze_table: str,
    silver_table: str,
    name_col: str,
    table_name: str,
    batch_id: str,
) -> DataFrame:
    bronze = spark.table(bronze_table).withColumn("rejection_reason", dim_rejection_reason(name_col))
    quality_rejected = bronze.filter(F.col("rejection_reason") != "")
    ranked = rank_by_id(bronze.filter(F.col("rejection_reason") == ""))
    winners = ranked.filter(F.col("_rn") == 1)
    dup_losers = ranked.filter(F.col("_rn") > 1).withColumn(
        "rejection_reason", F.lit("duplicate_id_loser")
    )

    silver = winners.select("id", name_col)
    overwrite_delta(silver, silver_table)

    rejected = to_rejected_records(
        quality_rejected.unionByName(dup_losers, allowMissingColumns=True),
        table_name,
        batch_id,
    )
    persist_rejected(spark, rejected, table_name, batch_id)
    return silver


def transform_departments(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    return _clean_dimension(
        spark,
        bronze_table="bronze.departments_raw",
        silver_table=SILVER_DEPARTMENTS,
        name_col="department",
        table_name="departments",
        batch_id=batch_id or BATCH_ID,
    )


def transform_jobs(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    return _clean_dimension(
        spark,
        bronze_table="bronze.jobs_raw",
        silver_table=SILVER_JOBS,
        name_col="job",
        table_name="jobs",
        batch_id=batch_id or BATCH_ID,
    )


def transform_dimensions(spark: SparkSession, batch_id: str | None = None) -> None:
    transform_departments(spark, batch_id)
    transform_jobs(spark, batch_id)


def _is_blank(column: str):
    col = F.col(column)
    return col.isNull() | (F.trim(col.cast("string")) == "")


def parse_hired_at(datetime_col: str = "datetime"):
    raw = F.col(datetime_col)
    return F.coalesce(
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(raw, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(raw, "yyyy-MM-dd HH:mm:ss"),
    )


def type_hired_employees(spark: SparkSession) -> DataFrame:
    """Cast Bronze employees, parse hired_at, and flag quality issues (no FK)."""
    df = (
        spark.table("bronze.hired_employees_raw")
        .withColumn("id", F.when(_is_blank("id"), F.lit(None).cast("int")).otherwise(F.col("id").cast("int")))
        .withColumn(
            "name",
            F.when(_is_blank("name"), F.lit(None).cast("string")).otherwise(F.trim(F.col("name"))),
        )
        .withColumn(
            "department_id",
            F.when(_is_blank("department_id"), F.lit(None).cast("int")).otherwise(
                F.col("department_id").cast("int")
            ),
        )
        .withColumn(
            "job_id",
            F.when(_is_blank("job_id"), F.lit(None).cast("int")).otherwise(F.col("job_id").cast("int")),
        )
        .withColumn("hired_at", parse_hired_at())
        .withColumn("flag_null_id", F.col("id").isNull())
        .withColumn("flag_null_or_empty_name", F.col("name").isNull())
        .withColumn("flag_invalid_datetime", F.col("hired_at").isNull())
        .withColumn("flag_null_department_id", F.col("department_id").isNull())
        .withColumn("flag_null_job_id", F.col("job_id").isNull())
        .withColumn(
            "quality_reason",
            F.concat_ws(
                "|",
                F.when(F.col("flag_null_id"), F.lit("null_id")),
                F.when(F.col("flag_null_or_empty_name"), F.lit("null_or_empty_name")),
                F.when(F.col("flag_invalid_datetime"), F.lit("invalid_datetime")),
                F.when(F.col("flag_null_department_id"), F.lit("null_department_id")),
                F.when(F.col("flag_null_job_id"), F.lit("null_job_id")),
            ),
        )
    )
    df.createOrReplaceTempView("employees_typed")
    return df


def apply_employee_fk_and_split(
    spark: SparkSession,
    typed: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Join dims for FK checks and split valid candidates vs quality rejects."""
    typed_df = typed if typed is not None else type_hired_employees(spark)
    ensure_table_registered(spark, SILVER_DEPARTMENTS)
    ensure_table_registered(spark, SILVER_JOBS)

    departments = spark.table(SILVER_DEPARTMENTS).select(F.col("id").alias("_dept_pk"))
    jobs = spark.table(SILVER_JOBS).select(F.col("id").alias("_job_pk"))

    with_fk = (
        typed_df.join(departments, typed_df.department_id == F.col("_dept_pk"), "left")
        .join(jobs, typed_df.job_id == F.col("_job_pk"), "left")
        .withColumn(
            "flag_fk_department",
            F.col("department_id").isNotNull() & F.col("_dept_pk").isNull(),
        )
        .withColumn(
            "flag_fk_job",
            F.col("job_id").isNotNull() & F.col("_job_pk").isNull(),
        )
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                "|",
                F.when(F.col("quality_reason") != "", F.col("quality_reason")),
                F.when(F.col("flag_fk_department"), F.lit("fk_department")),
                F.when(F.col("flag_fk_job"), F.lit("fk_job")),
            ),
        )
        .drop("_dept_pk", "_job_pk")
    )

    valid = with_fk.filter(F.col("rejection_reason") == "")
    rejected = with_fk.filter(F.col("rejection_reason") != "")
    valid.createOrReplaceTempView("employees_valid_candidates")
    rejected.createOrReplaceTempView("employees_quality_rejected")
    return valid, rejected


def dedup_hired_employees(
    spark: SparkSession,
    valid_candidates: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Keep one row per employee id; mark losers as duplicate_id_loser."""
    candidates = valid_candidates
    if candidates is None:
        candidates, _ = apply_employee_fk_and_split(spark)

    ranked = rank_by_id(candidates)
    winners = ranked.filter(F.col("_rn") == 1).drop("_rn")
    losers = (
        ranked.filter(F.col("_rn") > 1)
        .withColumn("rejection_reason", F.lit("duplicate_id_loser"))
        .drop("_rn")
    )
    winners.createOrReplaceTempView("employees_winners")
    losers.createOrReplaceTempView("employees_duplicate_losers")
    return winners, losers


def _silver_employee_rows(winners: DataFrame) -> DataFrame:
    return winners.select(*SILVER_EMPLOYEE_COLS)


def merge_employee_winners(spark: SparkSession, winners: DataFrame) -> None:
    source = _silver_employee_rows(winners)
    if ensure_table_registered(spark, SILVER_HIRED_EMPLOYEES):
        updates = {col: f"s.{col}" for col in SILVER_EMPLOYEE_COLS if col != "id"}
        (
            DeltaTable.forName(spark, SILVER_HIRED_EMPLOYEES)
            .alias("t")
            .merge(source.alias("s"), "t.id = s.id")
            .whenMatchedUpdate(condition=_NEWER_SOURCE, set=updates)
            .whenNotMatchedInsertAll()
            .execute()
        )
        return
    overwrite_delta(source, SILVER_HIRED_EMPLOYEES)


def transform_hired_employees(spark: SparkSession, batch_id: str | None = None) -> DataFrame:
    """E1–E4: type, FK split, dedup, MERGE winners, persist rejected."""
    bid = batch_id or BATCH_ID
    valid, quality_rejected = apply_employee_fk_and_split(spark)
    winners, losers = dedup_hired_employees(spark, valid)
    merge_employee_winners(spark, winners)

    rejected_source = quality_rejected.unionByName(losers, allowMissingColumns=True)
    persist_rejected(
        spark,
        to_rejected_records(rejected_source, "hired_employees", bid),
        "hired_employees",
        bid,
    )
    return winners



