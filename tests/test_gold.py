"""Gold aggregates on a tiny hand-computed fixture."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal as D

import pytest

from conftest import changes_df, cust
from lakehouse.gold import customer_360, daily_account_summary
from lakehouse.silver import CUSTOMER_ATTRS, build_scd2

TXN_SCHEMA = (
    "txn_id string, account_id string, txn_ts timestamp, txn_date date, amount decimal(18,2), "
    "signed_amount decimal(18,2), currency string, direction string, merchant_category string, channel string"
)
ACCT_SCHEMA = "account_id string, customer_id string, account_type string, status string, is_deleted boolean"


def _txn(tid, acct, day, amount, direction, mcc=None):
    amt = D(amount)
    return (tid, acct, datetime(2026, 1, day, 12), date(2026, 1, day), amt,
            -amt if direction == "DEBIT" else amt, "USD", direction, mcc, "POS")  # fmt: skip


@pytest.fixture
def fixture(spark):
    accounts = spark.createDataFrame([
        ("A1", "C1", "CHECKING", "ACTIVE", False),
        ("A2", "C1", "SAVINGS", "CLOSED", False),
        ("A3", "C2", "CHECKING", "ACTIVE", False),
    ], ACCT_SCHEMA)  # fmt: skip
    txns = spark.createDataFrame([
        _txn("t1", "A1", 1, "1000.00", "CREDIT"),
        _txn("t2", "A1", 1, "100.00", "DEBIT", "GROCERY"),
        _txn("t3", "A1", 2, "50.00", "DEBIT", "FUEL"),
        _txn("t4", "A2", 1, "200.00", "CREDIT"),
        _txn("t5", "A2", 2, "200.00", "DEBIT", "TRAVEL"),
        _txn("t6", "A3", 2, "30.00", "DEBIT", "GROCERY"),
    ], TXN_SCHEMA)  # fmt: skip
    dim = build_scd2(changes_df(spark, [
        cust("e1", "I", "2026-01-01T00:00:00Z", cid="C1"),
        cust("e2", "U", "2026-01-02T00:00:00Z", cid="C1", segment="premier"),
        cust("e3", "I", "2026-01-01T00:00:00Z", cid="C2"),
    ]), "customer_id", CUSTOMER_ATTRS)  # fmt: skip
    return accounts, txns, dim


def test_daily_account_summary(fixture):
    accounts, txns, _ = fixture
    out = {(r.account_id, r.txn_date.day): r for r in daily_account_summary(txns, accounts).collect()}

    assert set(out) == {("A1", 1), ("A1", 2), ("A2", 1), ("A2", 2), ("A3", 2)}
    a1d1 = out[("A1", 1)]
    assert (a1d1.txn_count, a1d1.total_credits, a1d1.total_debits, a1d1.net_amount, a1d1.closing_balance) == (
        2, D("1000.00"), D("100.00"), D("900.00"), D("900.00"),
    )  # fmt: skip
    assert out[("A1", 2)].closing_balance == D("850.00")  # running balance
    assert out[("A2", 2)].closing_balance == D("0.00")
    assert out[("A3", 2)].closing_balance == D("-30.00")
    assert out[("A1", 1)].customer_id == "C1"


def test_customer_360(fixture):
    accounts, txns, dim = fixture
    daily = daily_account_summary(txns, accounts)
    out = {r.customer_id: r for r in customer_360(dim, accounts, txns, daily).collect()}

    c1, c2 = out["C1"], out["C2"]
    assert c1.segment == "PREMIER" and c1.profile_versions == 2
    assert (c1.num_accounts, c1.num_active_accounts) == (2, 1)
    assert c1.total_balance == D("850.00")  # closed account A2 excluded
    assert (c1.txn_count, c1.total_spend, c1.spend_last_7d) == (5, D("350.00"), D("350.00"))
    assert c1.top_merchant_category == "TRAVEL"
    assert c1.as_of_date == date(2026, 1, 2)
    assert (c2.total_balance, c2.txn_count, c2.top_merchant_category) == (D("-30.00"), 1, "GROCERY")
