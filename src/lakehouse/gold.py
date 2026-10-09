"""Gold layer: business-level aggregates.

``daily_account_summary``  one row per account and day with activity: counts, credit /
                           debit totals, net movement and running ledger balance.
``customer_360``           one row per customer (current SCD2 version) with profile,
                           history depth, account portfolio, balances and spend.

Gold tables are fully recomputed from silver on every run. That keeps them trivially
idempotent and means late transactions automatically flow into balances. At larger
scale this would become an incremental recompute of affected (account, date) slices.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import storage
from lakehouse.config import PipelineConfig
from lakehouse.log import get_logger
from lakehouse.silver import first_per_group, silver_path

log = get_logger(__name__)

MONEY = "decimal(18,2)"


def gold_path(cfg: PipelineConfig, table: str) -> Path:
    return cfg.gold_dir / table


def daily_account_summary(transactions: DataFrame, accounts: DataFrame) -> DataFrame:
    daily = transactions.groupBy("account_id", "txn_date").agg(
        F.count("*").alias("txn_count"),
        F.coalesce(F.sum(F.when(F.col("direction") == "CREDIT", F.col("amount"))), F.lit(0))
        .cast(MONEY)
        .alias("total_credits"),
        F.coalesce(F.sum(F.when(F.col("direction") == "DEBIT", F.col("amount"))), F.lit(0))
        .cast(MONEY)
        .alias("total_debits"),
        F.sum("signed_amount").cast(MONEY).alias("net_amount"),
    )
    running = Window.partitionBy("account_id").orderBy("txn_date").rowsBetween(Window.unboundedPreceding, 0)
    daily = daily.withColumn("closing_balance", F.sum("net_amount").over(running).cast(MONEY))
    acct = accounts.select("account_id", "customer_id", "account_type")
    return daily.join(acct, "account_id", "left").select(
        "account_id",
        "customer_id",
        "account_type",
        "txn_date",
        "txn_count",
        "total_credits",
        "total_debits",
        "net_amount",
        "closing_balance",
    )


def customer_360(dim_customer: DataFrame, accounts: DataFrame, transactions: DataFrame, daily: DataFrame) -> DataFrame:
    current = dim_customer.filter("is_current")
    history = dim_customer.groupBy("customer_id").agg(
        F.count("*").alias("profile_versions"), F.min("effective_from").alias("customer_since")
    )
    live_accounts = accounts.filter(~F.col("is_deleted"))
    portfolio = live_accounts.groupBy("customer_id").agg(
        F.count("*").alias("num_accounts"),
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("num_active_accounts"),
    )
    latest_balance = first_per_group(daily, ["account_id"], [F.col("txn_date").desc()])
    balances = (
        latest_balance.join(live_accounts.filter(F.col("status") != "CLOSED").select("account_id"), "account_id")
        .groupBy("customer_id")
        .agg(F.sum("closing_balance").cast(MONEY).alias("total_balance"))
    )
    txn = transactions.join(accounts.select("account_id", "customer_id"), "account_id")
    as_of = transactions.agg(F.max("txn_date").alias("as_of_date"))
    txn = txn.crossJoin(as_of)
    debit = F.col("direction") == "DEBIT"
    activity = txn.groupBy("customer_id").agg(
        F.count("*").alias("txn_count"),
        F.coalesce(F.sum(F.when(debit, F.col("amount"))), F.lit(0)).cast(MONEY).alias("total_spend"),
        F.coalesce(F.sum(F.when(debit & (F.col("txn_date") > F.date_sub("as_of_date", 7)), F.col("amount"))), F.lit(0))
        .cast(MONEY)
        .alias("spend_last_7d"),
        F.max("txn_ts").alias("last_txn_ts"),
    )
    by_category = (
        txn.filter(debit & F.col("merchant_category").isNotNull())
        .groupBy("customer_id", "merchant_category")
        .agg(F.sum("amount").alias("_spend"))
    )
    top_category = first_per_group(
        by_category, ["customer_id"], [F.col("_spend").desc(), F.col("merchant_category").asc()]
    ).select("customer_id", F.col("merchant_category").alias("top_merchant_category"))

    return (
        current.join(history, "customer_id", "left")
        .join(portfolio, "customer_id", "left")
        .join(balances, "customer_id", "left")
        .join(activity, "customer_id", "left")
        .join(top_category, "customer_id", "left")
        .crossJoin(as_of)
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "segment",
            "risk_rating",
            "is_deleted",
            "customer_since",
            "profile_versions",
            F.coalesce("num_accounts", F.lit(0)).alias("num_accounts"),
            F.coalesce("num_active_accounts", F.lit(0)).alias("num_active_accounts"),
            F.coalesce("total_balance", F.lit(0)).cast(MONEY).alias("total_balance"),
            F.coalesce("txn_count", F.lit(0)).alias("txn_count"),
            F.coalesce("total_spend", F.lit(0)).cast(MONEY).alias("total_spend"),
            F.coalesce("spend_last_7d", F.lit(0)).cast(MONEY).alias("spend_last_7d"),
            "top_merchant_category",
            "last_txn_ts",
            "as_of_date",
        )
    )


def build_all(spark: SparkSession, cfg: PipelineConfig) -> dict[str, int]:
    fmt = cfg.storage_format
    txns = storage.read_table(spark, silver_path(cfg, "transactions"), fmt)
    accounts = storage.read_table(spark, silver_path(cfg, "accounts"), fmt)
    dim = storage.read_table(spark, silver_path(cfg, "dim_customer"), fmt)
    if txns is None or accounts is None or dim is None:
        log.warning("gold: silver tables missing, skipping gold build")
        return {}
    daily = daily_account_summary(txns, accounts)
    storage.overwrite_table(daily, gold_path(cfg, "daily_account_summary"), fmt)
    daily = storage.read_table(spark, gold_path(cfg, "daily_account_summary"), fmt)
    c360 = customer_360(dim, accounts, txns, daily)
    storage.overwrite_table(c360, gold_path(cfg, "customer_360"), fmt)
    counts = {
        "daily_account_summary": daily.count(),
        "customer_360": storage.read_table(spark, gold_path(cfg, "customer_360"), fmt).count(),
    }
    log.info("gold: %s", counts)
    return counts
