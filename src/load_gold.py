"""Build Gold analytical tables from SparkSQL scripts."""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import SparkSession

from src.config import PROJECT_ROOT
from src.ingest_bronze import ensure_table_registered, table_location

GOLD_SQL_PATH = PROJECT_ROOT / "sql" / "gold_metrics.sql"

_SILVER_INPUTS = (
    "silver.hired_employees",
    "silver.departments",
    "silver.jobs",
)

_GOLD_TABLES = (
    "gold.hires_by_department_job_quarter_2021",
    "gold.departments_above_average_hiring_2021",
)


def _sql_statements(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    statements: list[str] = []
    for chunk in text.split(";"):
        lines = [
            line
            for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        statement = "\n".join(lines).strip()
        if statement:
            statements.append(statement)
    return statements


def run_sql_file(spark: SparkSession, path: Path) -> None:
    for statement in _sql_statements(path):
        spark.sql(statement)


def _purge_table(spark: SparkSession, qualified_name: str) -> None:
    """Drop catalog entry and remove warehouse files.

    Tables re-registered with LOCATION are external; DROP alone leaves the path
    and the next CREATE TABLE AS SELECT fails with NON_EMPTY_LOCATION.
    """
    if ensure_table_registered(spark, qualified_name):
        spark.sql(f"DROP TABLE IF EXISTS {qualified_name}")
    path = Path(table_location(qualified_name))
    if path.exists():
        shutil.rmtree(path)


def load_gold(spark: SparkSession, sql_path: Path | None = None) -> None:
    for table in _SILVER_INPUTS:
        ensure_table_registered(spark, table)
    for table in _GOLD_TABLES:
        _purge_table(spark, table)
    run_sql_file(spark, sql_path or GOLD_SQL_PATH)
