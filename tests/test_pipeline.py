"""End-to-end: idempotent re-runs and batch-delivery independence on generated data."""

from __future__ import annotations

import shutil

import pytest

from conftest import rows
from lakehouse import storage
from lakehouse.generate import GeneratorConfig, generate
from lakehouse.gold import gold_path
from lakehouse.pipeline import GOLD_TABLES, SILVER_TABLES, run_pipeline
from lakehouse.silver import silver_path

SMALL = GeneratorConfig(seed=7, customers=25, days=9, batches=3, malformed_per_file=1)


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    out = tmp_path_factory.mktemp("generated")
    generate(out, SMALL)
    return out


def snapshot(spark, cfg) -> dict[str, list[tuple]]:
    snap = {}
    for t in SILVER_TABLES:
        snap[f"silver.{t}"] = rows(storage.read_table(spark, silver_path(cfg, t), "parquet"))
    for t in GOLD_TABLES:
        snap[f"gold.{t}"] = rows(storage.read_table(spark, gold_path(cfg, t), "parquet"))
    return snap


def test_end_to_end_and_idempotent_rerun(spark, cfg, generated):
    shutil.copytree(generated, cfg.landing_dir)

    first = run_pipeline(cfg, spark)
    assert first["status"] == "SUCCEEDED"
    assert first["dq"]["failed"] == 0
    counts = first["table_counts"]
    assert counts["gold.customer_360"] == SMALL.customers
    assert counts["bronze._quarantine.transactions"] == SMALL.batches * SMALL.malformed_per_file
    before = snapshot(spark, cfg)

    second = run_pipeline(cfg, spark)
    assert all(b["files_ingested"] == 0 for b in second["bronze"].values())
    assert all(s["skipped"] for s in second["silver"].values())
    assert second["table_counts"] == counts
    assert snapshot(spark, cfg) == before


def test_incremental_delivery_matches_single_batch(spark, cfg, generated, tmp_path):
    # Reference: everything delivered at once.
    shutil.copytree(generated, cfg.landing_dir)
    run_pipeline(cfg, spark)
    reference = snapshot(spark, cfg)

    # Same events delivered batch by batch (late + duplicate events cross batches).
    inc = cfg.with_overrides(base_dir=tmp_path / "lake_inc", landing_dir=tmp_path / "landing_inc")
    for b in range(1, SMALL.batches + 1):
        for entity in ("customers", "accounts", "transactions"):
            src = generated / entity / f"{entity}_batch_{b:03d}.jsonl"
            (inc.landing_dir / entity).mkdir(parents=True, exist_ok=True)
            shutil.copy(src, inc.landing_dir / entity / src.name)
        assert run_pipeline(inc, spark)["status"] == "SUCCEEDED"

    assert snapshot(spark, inc) == reference
