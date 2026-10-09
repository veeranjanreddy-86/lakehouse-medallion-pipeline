"""Thin storage abstraction over Parquet (default) and Delta Lake.

Overwrites in Parquet mode go through a staging directory followed by a directory
swap. This lets a job safely rebuild a table from a DataFrame that lazily reads the
same table (Spark would otherwise delete its own input mid-write) and keeps readers
from ever seeing a half-written table. Delta gets the same guarantee from its log.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession


def table_exists(path: Path, fmt: str) -> bool:
    if fmt == "delta":
        return (path / "_delta_log").is_dir()
    return path.is_dir() and any(path.glob("*.parquet"))


def read_table(spark: SparkSession, path: Path, fmt: str) -> DataFrame | None:
    if not table_exists(path, fmt):
        return None
    return spark.read.format(fmt).load(str(path))


def append_table(df: DataFrame, path: Path, fmt: str) -> None:
    df.write.format(fmt).mode("append").save(str(path))


def overwrite_table(df: DataFrame, path: Path, fmt: str) -> None:
    if fmt == "delta":
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))
        return
    staging = path.with_name(path.name + ".__staging")
    retired = path.with_name(path.name + ".__retired")
    shutil.rmtree(staging, ignore_errors=True)
    shutil.rmtree(retired, ignore_errors=True)
    df.write.mode("overwrite").parquet(str(staging))
    if path.exists():
        path.rename(retired)
    staging.rename(path)
    shutil.rmtree(retired, ignore_errors=True)


# Small JSON checkpoints (ingestion log, processed bronze runs) ------------------
def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text())


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    tmp.replace(path)
