"""Pipeline orchestration: bronze -> silver -> DQ gate -> gold -> DQ -> reports.

Idempotency
-----------
* bronze ingests each landing file once (ingestion log);
* silver processes each bronze ingest run once (checkpoint) and, independently,
  anti-joins on event/txn ids, so even a replayed run cannot create duplicates;
* SCD2 / current-state tables are rebuilt per affected key from the change log,
  which is a deterministic function of the set of events seen;
* gold is fully recomputed from silver.

Re-running with no new landing files therefore changes nothing, and delivering the
same events in one batch or in several produces identical silver and gold tables.
"""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import bronze, gold, quality, silver, storage
from lakehouse.config import PipelineConfig
from lakehouse.log import get_logger, setup_logging
from lakehouse.quality import DQSuite
from lakehouse.schemas import ENTITIES
from lakehouse.spark import build_spark

log = get_logger(__name__)

SILVER_TABLES = ("customer_changes", "dim_customer", "account_changes", "accounts", "transactions")
GOLD_TABLES = ("daily_account_summary", "customer_360")


def new_run_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


def _read(spark: SparkSession, path: Path, fmt: str) -> DataFrame:
    df = storage.read_table(spark, path, fmt)
    if df is None:
        raise FileNotFoundError(f"expected table at {path}")
    return df


# ----------------------------------------------------------------------------
# Check catalogue
# ----------------------------------------------------------------------------
def run_silver_checks(spark: SparkSession, cfg: PipelineConfig, suite: DQSuite) -> None:
    fmt = cfg.storage_format
    q = quality
    bronze_t = {e.name: _read(spark, bronze.bronze_path(cfg, e.name), fmt) for e in ENTITIES}
    dim = _read(spark, silver.silver_path(cfg, "dim_customer"), fmt)
    changes = _read(spark, silver.silver_path(cfg, "customer_changes"), fmt)
    accounts = _read(spark, silver.silver_path(cfg, "accounts"), fmt)
    txns = _read(spark, silver.silver_path(cfg, "transactions"), fmt)

    # Bronze: quarantine rate (cumulative)
    for e in ENTITIES:
        qdf = storage.read_table(spark, bronze.quarantine_path(cfg, e.name), fmt)
        n_q = qdf.count() if qdf is not None else 0
        n_valid = bronze_t[e.name].count()
        suite.add(q.expect_ratio_at_most(
            f"bronze.{e.name}", "quarantine_ratio", n_q, n_q + n_valid, cfg.max_quarantine_ratio, "warn"
        ))  # fmt: skip

    # Reconciliation bronze -> silver (distinct business keys / event ids)
    def distinct(df: DataFrame, col: str) -> int:
        return df.select(F.upper(F.trim(col))).distinct().count()

    suite.add(
        q.expect_row_count_reconciliation(
            "silver.customer_changes",
            "bronze distinct event_id",
            distinct(bronze_t["customers"], "event_id"),
            changes.count(),
        )
    )
    suite.add(
        q.expect_row_count_reconciliation(
            "silver.dim_customer",
            "bronze distinct customer_id -> current rows",
            distinct(bronze_t["customers"], "customer_id"),
            dim.filter("is_current").count(),
        )
    )
    suite.add(
        q.expect_row_count_reconciliation(
            "silver.accounts",
            "bronze distinct account_id",
            distinct(bronze_t["accounts"], "account_id"),
            accounts.count(),
        )
    )
    suite.add(q.expect_row_count_reconciliation(
        "silver.transactions", "bronze distinct txn_id", distinct(bronze_t["transactions"], "txn_id"),
        txns.count(),
    ))  # fmt: skip

    # dim_customer (SCD2 invariants)
    t = "silver.dim_customer"
    suite.add(q.expect_not_null(dim, t, ["customer_id", "effective_from", "effective_to", "is_current"]))
    suite.add(q.expect_unique(dim, t, ["customer_id", "effective_from"]))
    suite.add(q.expect_unique(dim, t, ["customer_id"], where="is_current"))
    suite.add(q.expect_expression(dim, t, "valid_interval", "effective_from < effective_to"))
    w = Window.partitionBy("customer_id").orderBy("effective_from")
    contiguous = dim.withColumn("_next_from", F.lead("effective_from").over(w))
    suite.add(q.expect_expression(
        contiguous, t, "contiguous_history",
        "(_next_from IS NULL AND is_current) OR (_next_from = effective_to AND NOT is_current)",
    ))  # fmt: skip
    suite.add(q.expect_accepted_values(dim, t, "segment", ["RETAIL", "PREMIER", "PRIVATE", "SMALL_BUSINESS"]))
    suite.add(q.expect_accepted_values(dim, t, "risk_rating", ["LOW", "MEDIUM", "HIGH"]))
    suite.add(q.expect_expression(
        dim, t, "email_format", r"email IS NULL OR email RLIKE '^[^@\\s]+@[^@\\s]+\\.[a-z]{2,}$'", "warn"
    ))  # fmt: skip

    # accounts
    t = "silver.accounts"
    suite.add(q.expect_unique(accounts, t, ["account_id"]))
    suite.add(q.expect_not_null(accounts, t, ["customer_id", "account_type", "status"], where="NOT is_deleted"))
    suite.add(q.expect_accepted_values(accounts, t, "status", ["ACTIVE", "FROZEN", "CLOSED"]))
    suite.add(q.expect_accepted_values(accounts, t, "account_type", ["CHECKING", "SAVINGS", "CREDIT_CARD"]))
    suite.add(q.expect_accepted_values(accounts, t, "currency", ["USD"]))

    # transactions
    t = "silver.transactions"
    suite.add(q.expect_unique(txns, t, ["txn_id"]))
    suite.add(q.expect_not_null(txns, t, ["account_id", "txn_ts", "txn_date", "amount", "direction"]))
    suite.add(q.expect_expression(txns, t, "positive_amount", "amount > 0"))
    suite.add(q.expect_accepted_values(txns, t, "direction", ["DEBIT", "CREDIT"], allow_null=False))
    suite.add(q.expect_accepted_values(txns, t, "currency", ["USD"]))
    suite.add(q.expect_accepted_values(txns, t, "channel", ["POS", "ECOM", "ATM", "TRANSFER"]))
    known = accounts.select("account_id", F.lit(True).alias("_known_account"))
    suite.add(q.expect_expression(
        txns.join(known, "account_id", "left"), t, "referential_integrity(account_id)",
        "_known_account IS NOT NULL", "warn",
    ))  # fmt: skip


def run_gold_checks(spark: SparkSession, cfg: PipelineConfig, suite: DQSuite) -> None:
    fmt = cfg.storage_format
    q = quality
    daily = _read(spark, gold.gold_path(cfg, "daily_account_summary"), fmt)
    c360 = _read(spark, gold.gold_path(cfg, "customer_360"), fmt)
    txns = _read(spark, silver.silver_path(cfg, "transactions"), fmt)
    dim = _read(spark, silver.silver_path(cfg, "dim_customer"), fmt)

    t = "gold.daily_account_summary"
    suite.add(q.expect_unique(daily, t, ["account_id", "txn_date"]))
    suite.add(q.expect_not_null(daily, t, ["customer_id"], severity="warn"))
    suite.add(q.expect_expression(daily, t, "net_is_credits_minus_debits",
                                  "net_amount = total_credits - total_debits"))  # fmt: skip
    suite.add(q.expect_row_count_reconciliation(
        t, "sum(txn_count) vs silver.transactions", txns.count(),
        int(daily.agg(F.sum("txn_count")).first()[0] or 0),
    ))  # fmt: skip

    t = "gold.customer_360"
    suite.add(q.expect_unique(c360, t, ["customer_id"]))
    suite.add(q.expect_row_count_reconciliation(
        t, "rows vs current dim_customer", dim.filter("is_current").count(), c360.count()
    ))  # fmt: skip


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------
def table_counts(spark: SparkSession, cfg: PipelineConfig) -> dict[str, int | None]:
    fmt = cfg.storage_format
    paths: dict[str, Path] = {}
    for e in ENTITIES:
        paths[f"bronze.{e.name}"] = bronze.bronze_path(cfg, e.name)
        paths[f"bronze._quarantine.{e.name}"] = bronze.quarantine_path(cfg, e.name)
    paths.update({f"silver.{t}": silver.silver_path(cfg, t) for t in SILVER_TABLES})
    paths.update({f"gold.{t}": gold.gold_path(cfg, t) for t in GOLD_TABLES})
    out: dict[str, int | None] = {}
    for name, path in paths.items():
        df = storage.read_table(spark, path, fmt)
        out[name] = df.count() if df is not None else None
    return out


def run_pipeline(cfg: PipelineConfig, spark: SparkSession | None = None) -> dict[str, Any]:
    spark = spark or build_spark(cfg)
    run_id = new_run_id()
    suite = DQSuite(run_id=run_id, mode=cfg.dq_mode)
    started = datetime.now(UTC)
    log.info("pipeline run %s starting (format=%s, dq_mode=%s)", run_id, cfg.storage_format, cfg.dq_mode)
    summary: dict[str, Any] = {"run_id": run_id, "started_at": started.isoformat(timespec="seconds")}
    report_path = cfg.reports_dir / f"dq_report_{run_id}.json"
    try:
        bronze_results = {e.name: bronze.ingest_entity(spark, cfg, e, run_id) for e in ENTITIES}
        summary["bronze"] = {
            k: {"files_ingested": len(v.files_ingested), "files_skipped": v.files_skipped,
                "read": v.records_read, "valid": v.records_valid, "quarantined": v.records_quarantined}
            for k, v in bronze_results.items()
        }  # fmt: skip
        silver_results = silver.build_all(spark, cfg)
        summary["silver"] = {k: vars(v) for k, v in silver_results.items()}

        run_silver_checks(spark, cfg, suite)
        suite.enforce(stage="before gold publish")  # DQ gate: do not publish gold on bad silver

        summary["gold"] = gold.build_all(spark, cfg)
        run_gold_checks(spark, cfg, suite)
        suite.enforce(stage="after gold build")
        summary["status"] = "SUCCEEDED"
    except Exception:
        summary["status"] = "FAILED"
        raise
    finally:
        suite.write(report_path)
        suite.write(cfg.reports_dir / "dq_report_latest.json")
        summary["dq"] = suite.summary()
        summary["dq_report"] = str(report_path)
        summary["finished_at"] = datetime.now(UTC).isoformat(timespec="seconds")
        if summary["status"] == "SUCCEEDED":
            summary["table_counts"] = table_counts(spark, cfg)
        storage.save_json(cfg.reports_dir / f"run_summary_{run_id}.json", summary)
        storage.save_json(cfg.reports_dir / "run_summary_latest.json", summary)
        log.info("pipeline run %s %s; dq=%s", run_id, summary["status"], summary["dq"])
    return summary


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Run the bronze/silver/gold lakehouse pipeline.")
    p.add_argument("--config", help="TOML config (defaults to built-in settings)")
    p.add_argument("--format", dest="storage_format", choices=["parquet", "delta"])
    p.add_argument("--dq-mode", choices=["fail", "warn"])
    p.add_argument("--landing-dir", type=Path)
    p.add_argument("--base-dir", type=Path)
    args = p.parse_args(argv)
    overrides = {
        "storage_format": args.storage_format,
        "dq_mode": args.dq_mode,
        "landing_dir": args.landing_dir,
        "base_dir": args.base_dir,
    }
    if args.config:
        cfg = PipelineConfig.from_toml(args.config, **overrides)
    else:
        cfg = PipelineConfig().with_overrides(**overrides)
    setup_logging(cfg.log_level)
    summary = run_pipeline(cfg)
    print(json.dumps({k: summary[k] for k in ("run_id", "status", "dq", "table_counts") if k in summary}, indent=2))


if __name__ == "__main__":
    main()
