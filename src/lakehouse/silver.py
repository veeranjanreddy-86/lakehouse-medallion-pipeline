"""Silver layer: cleaned, deduplicated, conformed tables.

Tables
------
``customer_changes``  append-only, deduplicated CDC log for customers (source of truth)
``dim_customer``      SCD Type 2 dimension derived from ``customer_changes``
``account_changes``   append-only, deduplicated CDC log for accounts
``accounts``          current state per account (SCD Type 1, soft deletes)
``transactions``      deduplicated card/ledger transactions (insert-only fact)

Late and out-of-order events
----------------------------
Ordering is always by *event time* (``event_ts``), never by arrival. When new
change events arrive, only the affected business keys are rebuilt, from their full
change history. A late event therefore lands in the correct position of the
history and the downstream versions' ``effective_to`` are re-closed. Rebuilding from
the change log (rather than patching the dimension in place) is also what makes
no-op compression safe: a late event between two identical versions is re-evaluated
against the raw changes instead of against an already-compressed history.

All of this is plain DataFrame code, so it runs on Parquet without Delta. On Delta /
Databricks the "replace affected keys" step maps to ``MERGE INTO`` (see databricks/).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import storage
from lakehouse.bronze import bronze_path
from lakehouse.config import PipelineConfig
from lakehouse.log import get_logger
from lakehouse.schemas import parse_event_ts

log = get_logger(__name__)

HIGH_DATE = "9999-12-31 23:59:59"
CUSTOMER_ATTRS = ["first_name", "last_name", "email", "city", "segment", "risk_rating"]
ACCOUNT_ATTRS = ["customer_id", "account_type", "status", "currency", "opened_date"]
LINEAGE_COLS = ["_source_file", "_ingest_run_id", "_ingested_at"]


@dataclass
class SilverResult:
    table: str
    new_rows: int = 0
    keys_rebuilt: int = 0
    skipped: bool = False


def silver_path(cfg: PipelineConfig, table: str) -> Path:
    return cfg.silver_dir / table


# ----------------------------------------------------------------------------
# Generic helpers (pure DataFrame functions; unit tested directly)
# ----------------------------------------------------------------------------
def first_per_group(df: DataFrame, keys: list[str], order: list[Column]) -> DataFrame:
    """Keep exactly one row per ``keys`` - the first by ``order`` (deterministic)."""
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def dedupe_events(df: DataFrame, id_col: str = "event_id") -> DataFrame:
    """Drop re-delivered events (same id), keeping the earliest ingested copy."""
    order = [F.col(c).asc_nulls_last() for c in ("_ingested_at", "_source_file") if c in df.columns]
    return first_per_group(df, [id_col], order or [F.lit(1)])


def _forward_fill(df: DataFrame, key: str, attrs: list[str], order: list[Column]) -> DataFrame:
    """Attributes missing from a change image (e.g. deletes) inherit the prior value."""
    w = Window.partitionBy(key).orderBy(*order).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    for a in attrs:
        df = df.withColumn(a, F.last(a, ignorenulls=True).over(w))
    return df


def _ordered_timeline(changes: DataFrame, key: str, attrs: list[str]) -> tuple[DataFrame, list[Column]]:
    order = [F.col("event_ts").asc(), F.col("event_id").asc()]
    # Two different events for the same key at the same instant: highest id wins.
    changes = first_per_group(changes, [key, "event_ts"], [F.col("event_id").desc()])
    timeline = _forward_fill(changes, key, attrs, order).withColumn("is_deleted", F.col("op") == "D")
    return timeline, order


def build_scd2(changes: DataFrame, key: str, attrs: list[str]) -> DataFrame:
    """Build SCD2 versions from the full change history of a set of keys.

    * one version per effective change, ordered by event time;
    * consecutive versions with identical attributes (no-op updates, repeated
      deletes) are compressed;
    * ``effective_to`` of a version = ``effective_from`` of the next one, the open
      version gets ``9999-12-31 23:59:59`` and ``is_current = true``;
    * deletes are soft: the closing version has ``is_deleted = true``.
    """
    timeline, order = _ordered_timeline(changes, key, attrs)
    w = Window.partitionBy(key).orderBy(*order)
    row_hash = F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(a).cast("string"), F.lit("<null>")) for a in attrs],
                    F.col("is_deleted").cast("string")),
        256,
    )  # fmt: skip
    timeline = (
        timeline.withColumn("_row_hash", row_hash)
        .withColumn("_prev_hash", F.lag("_row_hash").over(w))
        .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("_row_hash")))
    )
    next_from = F.lead("event_ts").over(w)
    return timeline.select(
        F.substring(F.sha2(F.concat_ws("|", F.col(key), F.col("event_ts").cast("string")), 256), 1, 16).alias(
            f"{key.removesuffix('_id')}_sk"
        ),
        key,
        *attrs,
        "is_deleted",
        F.col("event_ts").alias("effective_from"),
        F.coalesce(next_from, F.to_timestamp(F.lit(HIGH_DATE))).alias("effective_to"),
        next_from.isNull().alias("is_current"),
        F.row_number().over(w).alias("version"),
        F.col("event_id").alias("_source_event_id"),
        F.col("op").alias("_change_op"),
    )


def build_current_state(changes: DataFrame, key: str, attrs: list[str]) -> DataFrame:
    """SCD1: latest state per key by event time (late events never overwrite newer ones)."""
    timeline, _ = _ordered_timeline(changes, key, attrs)
    latest = first_per_group(timeline, [key], [F.col("event_ts").desc(), F.col("event_id").desc()])
    return latest.select(
        key,
        *attrs,
        "is_deleted",
        F.col("event_ts").alias("last_event_ts"),
        F.col("event_id").alias("_source_event_id"),
    )


def standardize_customers(df: DataFrame) -> DataFrame:
    return df.select(
        F.upper(F.trim("customer_id")).alias("customer_id"),
        "event_id",
        F.upper("op").alias("op"),
        parse_event_ts("event_ts").alias("event_ts"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.initcap(F.trim("city")).alias("city"),
        F.upper(F.trim("segment")).alias("segment"),
        F.upper(F.trim("risk_rating")).alias("risk_rating"),
        *[c for c in LINEAGE_COLS if c in df.columns],
    )


def standardize_accounts(df: DataFrame) -> DataFrame:
    return df.select(
        F.upper(F.trim("account_id")).alias("account_id"),
        "event_id",
        F.upper("op").alias("op"),
        parse_event_ts("event_ts").alias("event_ts"),
        F.upper(F.trim("customer_id")).alias("customer_id"),
        F.upper(F.trim("account_type")).alias("account_type"),
        F.upper(F.trim("status")).alias("status"),
        F.upper(F.trim("currency")).alias("currency"),
        F.to_date("opened_date").alias("opened_date"),
        *[c for c in LINEAGE_COLS if c in df.columns],
    )


def standardize_transactions(df: DataFrame) -> DataFrame:
    amount = F.col("amount").cast("decimal(18,2)")
    direction = F.upper(F.trim("direction"))
    ts = parse_event_ts("txn_ts")
    return df.select(
        F.trim("txn_id").alias("txn_id"),
        F.upper(F.trim("account_id")).alias("account_id"),
        ts.alias("txn_ts"),
        F.to_date(ts).alias("txn_date"),
        amount.alias("amount"),
        F.when(direction == "DEBIT", -amount).otherwise(amount).cast("decimal(18,2)").alias("signed_amount"),
        F.upper(F.trim("currency")).alias("currency"),
        direction.alias("direction"),
        F.upper(F.trim("merchant_category")).alias("merchant_category"),
        F.upper(F.trim("channel")).alias("channel"),
        *[c for c in LINEAGE_COLS if c in df.columns],
    )


# ----------------------------------------------------------------------------
# Table builders (I/O)
# ----------------------------------------------------------------------------
def _checkpoint(cfg: PipelineConfig, table: str) -> Path:
    return cfg.checkpoint_dir / f"silver_{table}.json"


def _new_bronze_rows(spark: SparkSession, cfg: PipelineConfig, entity: str, table: str):
    """Bronze rows from ingest runs this silver table has not processed yet."""
    bronze = storage.read_table(spark, bronze_path(cfg, entity), cfg.storage_format)
    if bronze is None:
        return None, []
    done = set(storage.load_json(_checkpoint(cfg, table), {"processed_runs": []})["processed_runs"])
    runs = sorted(r[0] for r in bronze.select("_ingest_run_id").distinct().collect() if r[0] not in done)
    if not runs:
        return None, []
    return bronze.filter(F.col("_ingest_run_id").isin(runs)), runs


def _skip(res: SilverResult) -> SilverResult:
    res.skipped = True
    log.info("silver.%s: no unprocessed bronze runs, skipping", res.table)
    return res


def _mark_processed(cfg: PipelineConfig, table: str, runs: list[str]) -> None:
    state = storage.load_json(_checkpoint(cfg, table), {"processed_runs": []})
    state["processed_runs"] = sorted(set(state["processed_runs"]) | set(runs))
    storage.save_json(_checkpoint(cfg, table), state)


def _append_new_changes(spark, cfg, changes: DataFrame, log_table: str, id_col: str) -> DataFrame:
    """Append changes not yet present in the change log; return what was appended."""
    path = silver_path(cfg, log_table)
    existing = storage.read_table(spark, path, cfg.storage_format)
    fresh = dedupe_events(changes, id_col)
    if existing is not None:
        fresh = fresh.join(existing.select(id_col), id_col, "left_anti")
    # Materialise eagerly: `fresh` anti-joins against the change log, so lazily
    # re-evaluating it after the append below would (correctly!) yield nothing.
    fresh = fresh.localCheckpoint(eager=True)
    if fresh.limit(1).count():
        storage.append_table(fresh, path, cfg.storage_format)
    return fresh


def _rebuild_keys(spark, cfg, change_log: str, target: str, key: str, builder, attrs, fresh) -> int:
    keys = fresh.select(key).distinct()
    n_keys = keys.count()
    history = storage.read_table(spark, silver_path(cfg, change_log), cfg.storage_format)
    rebuilt = builder(history.join(keys, key, "left_semi"), key, attrs)
    existing = storage.read_table(spark, silver_path(cfg, target), cfg.storage_format)
    if existing is not None:
        rebuilt = existing.join(keys, key, "left_anti").unionByName(rebuilt)
    storage.overwrite_table(rebuilt, silver_path(cfg, target), cfg.storage_format)
    return n_keys


def build_customers(spark: SparkSession, cfg: PipelineConfig) -> SilverResult:
    res = SilverResult("dim_customer")
    new, runs = _new_bronze_rows(spark, cfg, "customers", "dim_customer")
    if new is None:
        return _skip(res)
    fresh = _append_new_changes(spark, cfg, standardize_customers(new), "customer_changes", "event_id")
    res.new_rows = fresh.count()
    if res.new_rows:
        res.keys_rebuilt = _rebuild_keys(
            spark, cfg, "customer_changes", "dim_customer", "customer_id", build_scd2, CUSTOMER_ATTRS, fresh
        )
    fresh.unpersist()
    _mark_processed(cfg, "dim_customer", runs)
    log.info("silver.dim_customer: new_changes=%d keys_rebuilt=%d", res.new_rows, res.keys_rebuilt)
    return res


def build_accounts(spark: SparkSession, cfg: PipelineConfig) -> SilverResult:
    res = SilverResult("accounts")
    new, runs = _new_bronze_rows(spark, cfg, "accounts", "accounts")
    if new is None:
        return _skip(res)
    fresh = _append_new_changes(spark, cfg, standardize_accounts(new), "account_changes", "event_id")
    res.new_rows = fresh.count()
    if res.new_rows:
        res.keys_rebuilt = _rebuild_keys(
            spark, cfg, "account_changes", "accounts", "account_id", build_current_state, ACCOUNT_ATTRS, fresh
        )
    fresh.unpersist()
    _mark_processed(cfg, "accounts", runs)
    log.info("silver.accounts: new_changes=%d keys_rebuilt=%d", res.new_rows, res.keys_rebuilt)
    return res


def build_transactions(spark: SparkSession, cfg: PipelineConfig) -> SilverResult:
    res = SilverResult("transactions")
    new, runs = _new_bronze_rows(spark, cfg, "transactions", "transactions")
    if new is None:
        return _skip(res)
    fresh = _append_new_changes(spark, cfg, standardize_transactions(new), "transactions", "txn_id")
    res.new_rows = fresh.count()
    fresh.unpersist()
    _mark_processed(cfg, "transactions", runs)
    log.info("silver.transactions: new_rows=%d", res.new_rows)
    return res


def build_all(spark: SparkSession, cfg: PipelineConfig) -> dict[str, SilverResult]:
    return {
        "dim_customer": build_customers(spark, cfg),
        "accounts": build_accounts(spark, cfg),
        "transactions": build_transactions(spark, cfg),
    }
