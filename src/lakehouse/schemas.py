"""Landing-zone contracts for each CDC entity.

Bronze enforces these schemas: unknown fields are dropped, type mismatches and
malformed JSON are routed to quarantine together with the raw line.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

CORRUPT_COL = "_corrupt_record"
VALID_OPS = ("I", "U", "D")


def _strings(*names: str) -> list[StructField]:
    return [StructField(n, StringType(), True) for n in names]


@dataclass(frozen=True)
class EntitySpec:
    name: str
    schema: StructType
    key: str
    ts_field: str
    required: tuple[str, ...]
    has_op: bool = True

    @property
    def schema_with_corrupt(self) -> StructType:
        return StructType([*self.schema.fields, StructField(CORRUPT_COL, StringType(), True)])


CUSTOMERS = EntitySpec(
    name="customers",
    schema=StructType(
        _strings(
            "event_id",
            "op",
            "event_ts",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "segment",
            "risk_rating",
        )
    ),
    key="customer_id",
    ts_field="event_ts",
    required=("event_id", "op", "event_ts", "customer_id"),
)

ACCOUNTS = EntitySpec(
    name="accounts",
    schema=StructType(
        _strings(
            "event_id",
            "op",
            "event_ts",
            "account_id",
            "customer_id",
            "account_type",
            "status",
            "currency",
            "opened_date",
        )
    ),
    key="account_id",
    ts_field="event_ts",
    required=("event_id", "op", "event_ts", "account_id"),
)

TRANSACTIONS = EntitySpec(
    name="transactions",
    schema=StructType(
        [
            *_strings("txn_id", "account_id", "txn_ts"),
            StructField("amount", DoubleType(), True),
            *_strings("currency", "direction", "merchant_category", "channel"),
        ]
    ),
    key="txn_id",
    ts_field="txn_ts",
    required=("txn_id", "account_id", "txn_ts", "amount", "direction"),
    has_op=False,
)

ENTITIES: tuple[EntitySpec, ...] = (CUSTOMERS, ACCOUNTS, TRANSACTIONS)


def parse_event_ts(col: Column | str) -> Column:
    """Parse the timestamp formats seen in the landing zone into a UTC timestamp.

    Accepts ISO-8601 with ``Z`` (``2026-01-03T10:15:00Z``), a space-separated form
    (``2026-01-03 10:15:00``) and 13-digit epoch milliseconds. Anything else -> null.
    """
    c = F.trim(F.col(col) if isinstance(col, str) else col)
    return F.coalesce(
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX"),
        F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        F.when(c.rlike(r"^\d{13}$"), F.timestamp_millis(c.cast("long"))),
    )
