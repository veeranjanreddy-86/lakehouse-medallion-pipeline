"""SCD Type 2 correctness: history rows, single current row, effective_to closure,
soft deletes, no-op compression and late/out-of-order events."""

from __future__ import annotations

import random
from datetime import datetime
from itertools import pairwise

from pyspark.sql import functions as F

from conftest import changes_df, cust, rows, write_landing
from lakehouse import bronze, silver, storage
from lakehouse.schemas import CUSTOMERS
from lakehouse.silver import CUSTOMER_ATTRS, HIGH_DATE, build_scd2

HIGH = datetime.fromisoformat(HIGH_DATE)


def dim_for(spark, events, key="C1"):
    dim = build_scd2(changes_df(spark, events), "customer_id", CUSTOMER_ATTRS)
    return sorted(dim.filter(F.col("customer_id") == key).collect(), key=lambda r: r.effective_from)


def test_history_rows_and_effective_to_closure(spark):
    versions = dim_for(spark, [
        cust("e1", "I", "2026-01-01T00:00:00Z"),
        cust("e2", "U", "2026-01-05T00:00:00Z", city="lakeside"),
        cust("e3", "U", "2026-01-09T00:00:00Z", city="lakeside", segment="premier"),
    ])  # fmt: skip

    assert [v.version for v in versions] == [1, 2, 3]
    assert [v.city for v in versions] == ["Riverton", "Lakeside", "Lakeside"]
    assert versions[-1].segment == "PREMIER"
    # each closed version ends exactly where the next begins; only the last is open
    for prev, nxt in pairwise(versions):
        assert prev.effective_to == nxt.effective_from
        assert prev.is_current is False
    assert versions[-1].is_current is True
    assert versions[-1].effective_to == HIGH
    assert len({v.customer_sk for v in versions}) == 3


def test_exactly_one_current_row_per_key(spark):
    events = []
    for k in range(1, 6):
        for n in range(1, k + 1):
            events.append(cust(f"e{k}-{n}", "I" if n == 1 else "U", f"2026-01-{n:02d}T00:00:00Z",
                               cid=f"C{k}", city=f"city{n}"))  # fmt: skip
    dim = build_scd2(changes_df(spark, events), "customer_id", CUSTOMER_ATTRS)

    per_key = {r.customer_id: r["count"] for r in dim.filter("is_current").groupBy("customer_id").count().collect()}
    assert per_key == {f"C{k}": 1 for k in range(1, 6)}
    assert dim.count() == 15


def test_input_order_does_not_matter(spark):
    events = [
        cust("e1", "I", "2026-01-01T00:00:00Z"),
        cust("e2", "U", "1767571200000", city="fairview"),  # epoch ms: 2026-01-05
        cust("e3", "U", "2026-01-07 08:30:00", risk_rating="high"),  # space-separated
        cust("e4", "D", "2026-01-09T00:00:00Z"),
    ]
    shuffled = events[:]
    random.Random(7).shuffle(shuffled)
    a = build_scd2(changes_df(spark, events), "customer_id", CUSTOMER_ATTRS)
    b = build_scd2(changes_df(spark, shuffled), "customer_id", CUSTOMER_ATTRS)
    assert rows(a) == rows(b)


def test_soft_delete_carries_attributes_and_reactivation(spark):
    versions = dim_for(spark, [
        cust("e1", "I", "2026-01-01T00:00:00Z"),
        cust("e2", "D", "2026-01-03T00:00:00Z"),
        cust("e3", "I", "2026-01-05T00:00:00Z", city="oakridge"),
    ])  # fmt: skip

    assert [(v._change_op, v.is_deleted, v.is_current) for v in versions] == [
        ("I", False, False),
        ("D", True, False),
        ("I", False, True),
    ]
    assert versions[1].email == "ada@example.com"  # delete image inherits last known attributes
    assert versions[2].city == "Oakridge"


def test_noop_updates_and_repeated_deletes_are_compressed(spark):
    versions = dim_for(spark, [
        cust("e1", "I", "2026-01-01T00:00:00Z"),
        cust("e2", "U", "2026-01-02T00:00:00Z"),  # identical image -> no new version
        cust("e3", "D", "2026-01-03T00:00:00Z"),
        cust("e4", "D", "2026-01-04T00:00:00Z"),  # repeated delete -> no new version
    ])  # fmt: skip
    assert [v._source_event_id for v in versions] == ["e1", "e3"]


def _ingest_and_build(spark, cfg, run_id):
    bronze.ingest_entity(spark, cfg, CUSTOMERS, run_id)
    silver.build_customers(spark, cfg)
    dim = storage.read_table(spark, silver.silver_path(cfg, "dim_customer"), "parquet")
    return sorted(dim.collect(), key=lambda r: (r.customer_id, r.effective_from))


def test_late_event_is_inserted_into_history_incrementally(spark, cfg):
    write_landing(cfg, "customers", "customers_batch_001.jsonl", [
        cust("e1", "I", "2026-01-01T00:00:00Z"),
        cust("e3", "U", "2026-01-10T00:00:00Z", city="lakeside"),
        cust("o1", "I", "2026-01-01T00:00:00Z", cid="C2"),
    ])  # fmt: skip
    before = _ingest_and_build(spark, cfg, "run-1")
    assert [(r.customer_id, r.city) for r in before] == [("C1", "Riverton"), ("C1", "Lakeside"), ("C2", "Riverton")]

    # Event e2 happened on Jan 5 but arrives after Jan 10's event.
    write_landing(cfg, "customers", "customers_batch_002.jsonl", [
        cust("e2", "U", "2026-01-05T00:00:00Z", city="hillcrest"),
        cust("e1", "I", "2026-01-01T00:00:00Z"),  # duplicate re-delivery
    ])  # fmt: skip
    after = _ingest_and_build(spark, cfg, "run-2")
    c1 = [r for r in after if r.customer_id == "C1"]

    assert [r.city for r in c1] == ["Riverton", "Hillcrest", "Lakeside"]
    assert c1[0].effective_to == datetime(2026, 1, 5)  # previous version re-closed
    assert c1[1].effective_to == datetime(2026, 1, 10)
    assert [r.is_current for r in c1] == [False, False, True]
    # untouched key is carried over unchanged
    assert [(r.customer_id, r.version) for r in after if r.customer_id == "C2"] == [("C2", 1)]


def test_late_event_between_identical_versions_is_not_lost(spark, cfg):
    """Compression must not swallow history: X@1, X@3 (no-op) then late Y@2 => X, Y, X."""
    write_landing(cfg, "customers", "customers_batch_001.jsonl", [
        cust("e1", "I", "2026-01-01T00:00:00Z"),
        cust("e3", "U", "2026-01-03T00:00:00Z"),
    ])  # fmt: skip
    assert len(_ingest_and_build(spark, cfg, "run-1")) == 1

    write_landing(cfg, "customers", "customers_batch_002.jsonl", [
        cust("e2", "U", "2026-01-02T00:00:00Z", city="maplewood"),
    ])  # fmt: skip
    after = _ingest_and_build(spark, cfg, "run-2")
    assert [r.city for r in after] == ["Riverton", "Maplewood", "Riverton"]
    assert [r._source_event_id for r in after] == ["e1", "e2", "e3"]
