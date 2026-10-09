"""Expectations fail on bad data; fail vs warn modes; JSON report; pipeline DQ gate."""

from __future__ import annotations

import json

import pytest

from conftest import cust, write_landing
from lakehouse import quality as q
from lakehouse import storage
from lakehouse.gold import gold_path
from lakehouse.pipeline import run_pipeline


@pytest.fixture
def bad_df(spark):
    return spark.createDataFrame(
        [("t1", 10.0, "DEBIT"), ("t1", -5.0, "CREDIT"), ("t2", None, "REFUND"), ("t3", 1.0, None)],
        "txn_id string, amount double, direction string",
    )


def test_expectations_detect_bad_data(bad_df):
    t = "silver.transactions"
    nn = q.expect_not_null(bad_df, t, ["amount"])
    uq = q.expect_unique(bad_df, t, ["txn_id"])
    av = q.expect_accepted_values(bad_df, t, "direction", ["DEBIT", "CREDIT"], allow_null=False)
    ex = q.expect_expression(bad_df, t, "positive_amount", "amount > 0")
    rc = q.expect_row_count_reconciliation(t, "bronze->silver", expected=4, actual=3)

    assert not nn.passed and nn.observed["null_counts"] == {"amount": 1}
    assert not uq.passed and uq.observed["duplicate_keys"] == 1
    assert not av.passed and av.observed["unexpected"] == {"REFUND": 1, "None": 1}
    assert not ex.passed and ex.observed["violations"] == 2  # -5.0 fails; NULL amount is not "> 0"
    assert not rc.passed and rc.observed["difference"] == 1
    assert all(r.status == "FAIL" for r in (nn, uq, av, ex, rc))


def test_expectations_pass_on_good_data(spark):
    good = spark.createDataFrame(
        [("t1", 1.0, "DEBIT"), ("t2", 2.0, "CREDIT")], "txn_id string, amount double, direction string"
    )
    checks = [
        q.expect_not_null(good, "t", ["txn_id", "amount"]),
        q.expect_unique(good, "t", ["txn_id"]),
        q.expect_accepted_values(good, "t", "direction", ["DEBIT", "CREDIT"]),
        q.expect_expression(good, "t", "positive", "amount > 0"),
        q.expect_row_count_reconciliation("t", "n", 2, 2),
        q.expect_unique(good, "t", ["direction"], where="amount > 1"),
    ]
    assert all(c.passed for c in checks)


def test_fail_mode_raises_warn_mode_does_not_and_report_is_written(bad_df, tmp_path):
    for mode in ("fail", "warn"):
        suite = q.DQSuite(run_id="r1", mode=mode)
        suite.add(q.expect_unique(bad_df, "t", ["txn_id"]))  # error -> FAIL
        suite.add(q.expect_not_null(bad_df, "t", ["direction"], severity="warn"))  # -> WARN
        report = json.loads(suite.write(tmp_path / f"{mode}.json").read_text())
        assert report["summary"] == {"total": 2, "passed": 0, "warned": 1, "failed": 1}
        assert [r["status"] for r in report["results"]] == ["FAIL", "WARN"]
        if mode == "fail":
            with pytest.raises(q.DataQualityError, match="unique"):
                suite.enforce()
        else:
            suite.enforce()  # no exception


def test_pipeline_dq_gate_blocks_gold_on_bad_silver(spark, cfg):
    """A negative amount passes bronze's structural checks but fails silver DQ."""
    write_landing(cfg, "customers", "customers_batch_001.jsonl", [cust("e1", "I", "2026-01-01T00:00:00Z")])
    write_landing(cfg, "accounts", "accounts_batch_001.jsonl", [{
        "event_id": "a1", "op": "I", "event_ts": "2026-01-01T00:00:00Z", "account_id": "A1", "customer_id": "C1",
        "account_type": "CHECKING", "status": "ACTIVE", "currency": "USD", "opened_date": "2026-01-01",
    }])  # fmt: skip
    write_landing(cfg, "transactions", "transactions_batch_001.jsonl", [{
        "txn_id": "t1", "account_id": "A1", "txn_ts": "2026-01-01T10:00:00Z", "amount": -42.0,
        "currency": "USD", "direction": "DEBIT", "merchant_category": "GROCERY", "channel": "POS",
    }])  # fmt: skip

    with pytest.raises(q.DataQualityError, match="positive_amount"):
        run_pipeline(cfg, spark)

    report = json.loads((cfg.reports_dir / "dq_report_latest.json").read_text())
    failed = [r["check"] for r in report["results"] if r["status"] == "FAIL"]
    assert failed == ["positive_amount: amount > 0"]
    assert storage.read_table(spark, gold_path(cfg, "customer_360"), "parquet") is None  # gold not published

    # Same data in warn mode completes and publishes gold.
    summary = run_pipeline(cfg.with_overrides(dq_mode="warn"), spark)
    assert summary["status"] == "SUCCEEDED"
    assert summary["table_counts"]["gold.customer_360"] == 1
