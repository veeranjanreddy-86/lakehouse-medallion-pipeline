"""Dedupe, late/out-of-order handling and standardisation in silver."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from conftest import write_landing
from lakehouse import bronze, silver, storage
from lakehouse.schemas import ACCOUNTS, TRANSACTIONS
from lakehouse.silver import ACCOUNT_ATTRS, build_current_state, dedupe_events, standardize_accounts


def txn(txn_id, ts, amount, direction="DEBIT", **kw):
    return {"txn_id": txn_id, "account_id": "a1", "txn_ts": ts, "amount": amount, "currency": "usd",
            "direction": direction.lower(), "merchant_category": "grocery", "channel": "pos", **kw}  # fmt: skip


def acct(event_id, op, ts, status="ACTIVE", **kw):
    image = {} if op == "D" else {"customer_id": "c1", "account_type": "checking", "status": status,
                                  "currency": "USD", "opened_date": "2026-01-01"}  # fmt: skip
    return {"event_id": event_id, "op": op, "event_ts": ts, "account_id": "A1", **image, **kw}


def accounts_df(spark, events):
    data = [tuple(e.get(f.name) for f in ACCOUNTS.schema.fields) for e in events]
    return standardize_accounts(spark.createDataFrame(data, ACCOUNTS.schema))


def test_dedupe_events_keeps_earliest_ingested_copy(spark):
    df = spark.createDataFrame(
        [("e1", "b.jsonl", datetime(2026, 1, 2)), ("e1", "a.jsonl", datetime(2026, 1, 1)), ("e2", "b.jsonl", None)],
        "event_id string, _source_file string, _ingested_at timestamp",
    )
    out = {r.event_id: r._source_file for r in dedupe_events(df).collect()}
    assert out == {"e1": "a.jsonl", "e2": "b.jsonl"}


def test_current_state_uses_event_time_not_arrival_order(spark):
    # The FROZEN event is newer but listed first (arrived first); the older ACTIVE
    # event arriving later must not overwrite it.
    state = build_current_state(
        accounts_df(
            spark, [acct("e2", "U", "2026-01-05T00:00:00Z", "FROZEN"), acct("e1", "I", "2026-01-01T00:00:00Z")]
        ),
        "account_id",
        ACCOUNT_ATTRS,
    ).collect()
    assert len(state) == 1
    assert state[0].status == "FROZEN"
    assert state[0].last_event_ts == datetime(2026, 1, 5)


def test_current_state_soft_delete_keeps_last_attributes(spark):
    state = build_current_state(
        accounts_df(spark, [acct("e1", "I", "2026-01-01T00:00:00Z"), acct("e2", "D", "2026-01-03T00:00:00Z")]),
        "account_id",
        ACCOUNT_ATTRS,
    ).first()
    assert state.is_deleted is True
    assert (state.customer_id, state.account_type, state.opened_date) == ("C1", "CHECKING", date(2026, 1, 1))


def test_late_account_event_does_not_regress_state_across_runs(spark, cfg):
    write_landing(cfg, "accounts", "accounts_batch_001.jsonl", [acct("e2", "U", "2026-01-05T00:00:00Z", "CLOSED")])
    bronze.ingest_entity(spark, cfg, ACCOUNTS, "run-1")
    silver.build_accounts(spark, cfg)
    write_landing(cfg, "accounts", "accounts_batch_002.jsonl", [acct("e1", "I", "2026-01-01T00:00:00Z")])
    bronze.ingest_entity(spark, cfg, ACCOUNTS, "run-2")
    silver.build_accounts(spark, cfg)

    state = storage.read_table(spark, silver.silver_path(cfg, "accounts"), "parquet").collect()
    assert [(s.status, s._source_event_id) for s in state] == [("CLOSED", "e2")]
    assert state[0].account_type == "CHECKING"  # attributes back-filled from the late insert


def test_transactions_dedupe_and_standardisation(spark, cfg):
    write_landing(cfg, "transactions", "transactions_batch_001.jsonl", [
        txn("t1", "2026-01-01T09:00:00Z", 10.006),
        txn("t2", "2026-01-01 23:59:59", 250.0, direction="CREDIT"),
        txn("t1", "2026-01-01T09:00:00Z", 10.006),  # duplicate in same file
    ])  # fmt: skip
    write_landing(cfg, "transactions", "transactions_batch_002.jsonl", [
        txn("t2", "2026-01-01 23:59:59", 250.0, direction="CREDIT"),  # duplicate across files
        txn("t3", "1767312000000", 5.0),  # epoch millis -> 2026-01-02T00:00:00Z
    ])  # fmt: skip
    bronze.ingest_entity(spark, cfg, TRANSACTIONS, "run-1")
    res = silver.build_transactions(spark, cfg)
    assert res.new_rows == 3

    out = {r.txn_id: r for r in storage.read_table(spark, silver.silver_path(cfg, "transactions"), "parquet").collect()}
    assert sorted(out) == ["t1", "t2", "t3"]
    assert out["t1"].amount == Decimal("10.01") and out["t1"].signed_amount == Decimal("-10.01")
    assert out["t2"].signed_amount == Decimal("250.00")
    assert (out["t1"].currency, out["t1"].direction, out["t1"].channel) == ("USD", "DEBIT", "POS")
    assert out["t3"].txn_ts == datetime(2026, 1, 2) and out["t3"].txn_date == date(2026, 1, 2)

    # Same events replayed in a new file -> still no duplicates.
    write_landing(cfg, "transactions", "transactions_batch_003.jsonl", [txn("t3", "1767312000000", 5.0)])
    bronze.ingest_entity(spark, cfg, TRANSACTIONS, "run-2")
    assert silver.build_transactions(spark, cfg).new_rows == 0
    assert storage.read_table(spark, silver.silver_path(cfg, "transactions"), "parquet").count() == 3
