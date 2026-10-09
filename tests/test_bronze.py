from __future__ import annotations

import json

from pyspark.sql import functions as F

from conftest import cust, write_landing
from lakehouse import bronze, storage
from lakehouse.schemas import CUSTOMERS, TRANSACTIONS


def _quarantine(spark, cfg, entity):
    return storage.read_table(spark, bronze.quarantine_path(cfg, entity), "parquet")


def test_schema_enforcement_and_quarantine_reasons(spark, cfg):
    write_landing(cfg, "customers", "customers_batch_001.jsonl", [
        cust("e1", "I", "2026-01-01T10:00:00Z"),
        {**cust("e2", "u", "2026-01-02 11:00:00"), "unexpected_field": "dropped"},  # lower-case op OK
        '{"event_id": "e3", "op": "U", "event_ts": "2026-01-0',                     # truncated JSON
        json.dumps({"event_id": "e4", "op": "U", "event_ts": "2026-01-03T00:00:00Z"}),  # no customer_id
        json.dumps(cust("e5", "X", "2026-01-03T00:00:00Z")),                        # invalid op
        json.dumps(cust("e6", "U", "03/01/2026 10am")),                             # bad timestamp
        "",                                                                         # blank lines ignored
    ])  # fmt: skip

    res = bronze.ingest_entity(spark, cfg, CUSTOMERS, "run-1")

    assert (res.records_read, res.records_valid, res.records_quarantined) == (6, 2, 4)
    valid = storage.read_table(spark, bronze.bronze_path(cfg, "customers"), "parquet")
    assert "unexpected_field" not in valid.columns
    assert {"_source_file", "_ingest_run_id", "_ingested_at"} <= set(valid.columns)
    assert sorted(r.op for r in valid.collect()) == ["I", "U"]
    assert {r._source_file for r in valid.collect()} == {"customers_batch_001.jsonl"}

    reasons = {r._quarantine_reason for r in _quarantine(spark, cfg, "customers").collect()}
    assert reasons == {
        "malformed_json_or_type_mismatch",
        "missing_required:customer_id",
        "invalid_op",
        "invalid_timestamp",
    }


def test_type_mismatch_is_quarantined_with_raw_line(spark, cfg):
    good = {"txn_id": "t1", "account_id": "A1", "txn_ts": "2026-01-01T09:00:00Z", "amount": 10.5,
            "currency": "USD", "direction": "DEBIT", "merchant_category": "GROCERY", "channel": "POS"}  # fmt: skip
    bad = {**good, "txn_id": "t2", "amount": "twelve"}
    write_landing(cfg, "transactions", "transactions_batch_001.jsonl", [good, bad])

    res = bronze.ingest_entity(spark, cfg, TRANSACTIONS, "run-1")

    assert (res.records_valid, res.records_quarantined) == (1, 1)
    q = _quarantine(spark, cfg, "transactions").first()
    assert q._quarantine_reason == "malformed_json_or_type_mismatch"
    assert json.loads(q._raw)["amount"] == "twelve"
    schema = dict(storage.read_table(spark, bronze.bronze_path(cfg, "transactions"), "parquet").dtypes)
    assert schema["amount"] == "double"


def test_reingesting_the_same_files_is_a_noop(spark, cfg):
    write_landing(cfg, "customers", "customers_batch_001.jsonl", [cust("e1", "I", "2026-01-01T10:00:00Z")])
    bronze.ingest_entity(spark, cfg, CUSTOMERS, "run-1")
    second = bronze.ingest_entity(spark, cfg, CUSTOMERS, "run-2")

    assert second.files_ingested == [] and second.files_skipped == 1
    table = storage.read_table(spark, bronze.bronze_path(cfg, "customers"), "parquet")
    assert table.count() == 1

    # A new file is picked up incrementally, old one still skipped.
    write_landing(cfg, "customers", "customers_batch_002.jsonl", [cust("e2", "U", "2026-01-02T10:00:00Z")])
    third = bronze.ingest_entity(spark, cfg, CUSTOMERS, "run-3")
    assert third.files_ingested == ["customers_batch_002.jsonl"]
    runs = {r[0] for r in storage.read_table(spark, bronze.bronze_path(cfg, "customers"), "parquet")
            .select(F.col("_ingest_run_id")).distinct().collect()}  # fmt: skip
    assert runs == {"run-1", "run-3"}
