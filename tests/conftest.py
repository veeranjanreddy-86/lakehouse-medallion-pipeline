from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from lakehouse.config import PipelineConfig
from lakehouse.schemas import CUSTOMERS
from lakehouse.silver import standardize_customers
from lakehouse.spark import build_spark

# Python converts collected timestamps to the local zone; pin it so assertions are UTC.
os.environ["TZ"] = "UTC"
time.tzset()

LINEAGE = {"_source_file", "_ingest_run_id", "_ingested_at"}


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = build_spark(
        PipelineConfig(spark_master="local[2]", shuffle_partitions=2, extra_spark_conf={
            "spark.default.parallelism": "2",
            "spark.sql.adaptive.enabled": "false",
        }),
        app_name="lakehouse-tests",
    )  # fmt: skip
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def cfg(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        base_dir=tmp_path / "lake",
        landing_dir=tmp_path / "landing",
        spark_master="local[2]",
        shuffle_partitions=2,
        dq_mode="fail",
    )


def write_landing(cfg: PipelineConfig, entity: str, filename: str, records: list[dict[str, Any] | str]) -> Path:
    """Write JSON-lines; dict records are serialised, str records written verbatim."""
    path = cfg.landing_dir / entity / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [r if isinstance(r, str) else json.dumps(r) for r in records]
    path.write_text("\n".join(lines) + "\n")
    return path


def rows(df: DataFrame, *, drop: set[str] = LINEAGE) -> list[tuple]:
    """Order-independent, lineage-free snapshot of a DataFrame for equality checks."""
    cols = sorted(c for c in df.columns if c not in drop)
    return sorted((tuple(r) for r in df.select(*cols).collect()), key=repr)


def cust(event_id: str, op: str, ts: str, cid: str = "C1", **attrs: Any) -> dict[str, Any]:
    base = {"first_name": "ada", "last_name": "lovelace", "email": "ada@example.com",
            "city": "riverton", "segment": "retail", "risk_rating": "low"}  # fmt: skip
    if op == "D":
        base = {}
    return {"event_id": event_id, "op": op, "event_ts": ts, "customer_id": cid, **base, **attrs}


def changes_df(spark: SparkSession, events: list[dict[str, Any]]) -> DataFrame:
    """Customer CDC events (as dicts) -> standardised silver change rows."""
    data = [tuple(e.get(f.name) for f in CUSTOMERS.schema.fields) for e in events]
    return standardize_customers(spark.createDataFrame(data, CUSTOMERS.schema))
