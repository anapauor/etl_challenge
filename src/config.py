"""Pipeline paths and batch identity.

Values follow the challenge contract. Override with env vars when needed.
CLI flags in ``run_pipeline`` (H1) will set the same env vars.
"""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_ROOT = Path(os.environ.get("DATA_ROOT", PROJECT_ROOT / "data"))
WAREHOUSE_DIR = Path(os.environ.get("WAREHOUSE_DIR", PROJECT_ROOT / "spark-warehouse"))
BATCH_ID = os.environ.get("BATCH_ID", "local")
APP_NAME = os.environ.get("APP_NAME", "hiring-etl")
