"""Local SparkSession with Delta Lake and bronze/silver/gold databases."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.config import APP_NAME, WAREHOUSE_DIR

_JAVA_CANDIDATES = (
    Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    Path("/opt/homebrew/opt/openjdk@17"),
    Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    Path("/usr/local/opt/openjdk@17"),
)

_DATABASES = ("bronze", "silver", "gold")


def _ensure_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        java_bin = Path(os.environ["JAVA_HOME"]) / "bin" / "java"
        if java_bin.exists():
            return
    for home in _JAVA_CANDIDATES:
        if (home / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = str(home)
            os.environ["PATH"] = f"{home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
            return


def _ensure_pyspark_python() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def get_spark(app_name: str | None = None) -> SparkSession:
    """Build a local[*] Spark session, enable Delta, create lakehouse DBs."""
    _ensure_java_home()
    _ensure_pyspark_python()

    warehouse = str(WAREHOUSE_DIR.resolve())
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name or APP_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for database in _DATABASES:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    return spark
