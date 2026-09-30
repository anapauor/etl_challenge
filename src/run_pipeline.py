"""CLI entrypoint: Bronze → Silver → Gold, then print validation counts."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the hiring ETL lakehouse pipeline.")
    parser.add_argument(
        "--data-root",
        default="data",
        help="Path to data/ with landing/ and api/ (default: data)",
    )
    parser.add_argument(
        "--batch-id",
        default=os.environ.get("BATCH_ID", "local"),
        help="Batch id for idempotent Bronze/rejected writes",
    )
    parser.add_argument(
        "--warehouse-dir",
        default=None,
        help="Optional Spark warehouse directory (default: ./spark-warehouse)",
    )
    return parser.parse_args(argv)


def _apply_runtime_env(args: argparse.Namespace) -> None:
    data_root = Path(args.data_root).resolve()
    os.environ["DATA_ROOT"] = str(data_root)
    os.environ["BATCH_ID"] = args.batch_id
    if args.warehouse_dir:
        os.environ["WAREHOUSE_DIR"] = str(Path(args.warehouse_dir).resolve())


def _print_validation(spark) -> None:
    from src.config import PROJECT_ROOT
    from src.ingest_bronze import ensure_table_registered
    from src.load_gold import _sql_statements

    tables = [
        "bronze.departments_raw",
        "bronze.jobs_raw",
        "bronze.hired_employees_raw",
        "silver.departments",
        "silver.jobs",
        "silver.hired_employees",
        "silver.rejected_records",
        "gold.hires_by_department_job_quarter_2021",
        "gold.departments_above_average_hiring_2021",
    ]
    for table in tables:
        ensure_table_registered(spark, table)

    counts_sql, dups_sql = _sql_statements(PROJECT_ROOT / "sql" / "validation.sql")
    print("=== validation counts ===")
    spark.sql(counts_sql).show(truncate=False)
    print("=== silver.hired_employees duplicate ids (expect 0 rows) ===")
    spark.sql(dups_sql).show(truncate=False)


def run(args: argparse.Namespace) -> None:
    _apply_runtime_env(args)

    # Import after env vars are set so config/DATA_ROOT/BATCH_ID pick them up.
    from src.ingest_bronze import ingest_dimensions, ingest_hired_employees
    from src.load_gold import load_gold
    from src.spark_session import get_spark
    from src.transform_silver import transform_dimensions, transform_hired_employees

    batch_id = args.batch_id
    spark = get_spark()
    try:
        print(f"Running pipeline batch_id={batch_id} data_root={os.environ['DATA_ROOT']}")
        ingest_dimensions(spark, batch_id)
        ingest_hired_employees(spark, batch_id)
        transform_dimensions(spark, batch_id)
        transform_hired_employees(spark, batch_id)
        load_gold(spark)
        _print_validation(spark)
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
