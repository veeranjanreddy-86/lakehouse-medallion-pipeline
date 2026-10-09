"""Lightweight data-quality expectations with a JSON report.

Each check has a severity: ``error`` or ``warn``. The suite runs in one of two modes:

* ``fail`` - after the report is written, raise :class:`DataQualityError` if any
  ``error``-severity check failed;
* ``warn`` - log failures but never raise.

The API intentionally mirrors Delta Live Tables expectations (``expect``,
``expect_or_drop``, ``expect_or_fail``) so checks can be ported 1:1 (see databricks/).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.log import get_logger

log = get_logger(__name__)

SEVERITIES = ("error", "warn")


class DataQualityError(RuntimeError):
    """Raised in ``fail`` mode when an error-severity expectation fails."""


@dataclass
class CheckResult:
    table: str
    check: str
    severity: str
    passed: bool
    observed: dict[str, Any] = field(default_factory=dict)

    @property
    def status(self) -> str:
        if self.passed:
            return "PASS"
        return "FAIL" if self.severity == "error" else "WARN"


def _apply_where(df: DataFrame, where: str | None) -> DataFrame:
    return df.filter(where) if where else df


def expect_not_null(df: DataFrame, table: str, columns: list[str], severity="error", where=None) -> CheckResult:
    scoped = _apply_where(df, where)
    row = scoped.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]).first()
    nulls = {c: int(row[c] or 0) for c in columns}
    return CheckResult(
        table, f"not_null({', '.join(columns)})", severity, not any(nulls.values()), {"null_counts": nulls}
    )


def expect_unique(df: DataFrame, table: str, columns: list[str], severity="error", where=None) -> CheckResult:
    scoped = _apply_where(df, where)
    dupes = scoped.groupBy(*columns).count().filter("count > 1")
    n_dupe_keys = dupes.count()
    sample = [r.asDict() for r in dupes.limit(3).collect()]
    label = f"unique({', '.join(columns)})" + (f" where {where}" if where else "")
    return CheckResult(table, label, severity, n_dupe_keys == 0, {"duplicate_keys": n_dupe_keys, "sample": sample})


def expect_accepted_values(
    df: DataFrame, table: str, column: str, values: list[Any], severity="error", allow_null=True
) -> CheckResult:
    bad = df.filter(~F.col(column).isin(*values) | (F.col(column).isNull() if not allow_null else F.lit(False)))
    counts = {str(r[column]): r["count"] for r in bad.groupBy(column).count().limit(10).collect()}
    return CheckResult(
        table, f"accepted_values({column})", severity, not counts, {"unexpected": counts, "accepted": list(values)}
    )


def expect_expression(df: DataFrame, table: str, name: str, condition: str, severity="error") -> CheckResult:
    """Every row must satisfy the SQL ``condition``; NULL results count as violations."""
    violations = df.filter(f"NOT coalesce(({condition}), false)").count()
    return CheckResult(table, f"{name}: {condition}", severity, violations == 0, {"violations": violations})


def expect_row_count_reconciliation(
    table: str, name: str, expected: int, actual: int, severity="error", tolerance: float = 0.0
) -> CheckResult:
    diff = abs(expected - actual)
    allowed = int(expected * tolerance)
    return CheckResult(
        table,
        f"row_count_reconciliation({name})",
        severity,
        diff <= allowed,
        {"expected": expected, "actual": actual, "difference": diff, "tolerance": tolerance},
    )


def expect_ratio_at_most(table: str, name: str, numerator: int, denominator: int, max_ratio: float, severity="warn"):
    ratio = numerator / denominator if denominator else 0.0
    return CheckResult(
        table,
        f"{name} <= {max_ratio:.2%}",
        severity,
        ratio <= max_ratio,
        {"numerator": numerator, "denominator": denominator, "ratio": round(ratio, 5)},
    )


@dataclass
class DQSuite:
    run_id: str
    mode: str = "fail"
    results: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> CheckResult:
        if result.severity not in SEVERITIES:
            raise ValueError(f"unknown severity {result.severity!r}")
        self.results.append(result)
        level = {"PASS": log.debug, "WARN": log.warning, "FAIL": log.error}[result.status]
        level("DQ %s %s.%s %s", result.status, result.table, result.check, result.observed)
        return result

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == "FAIL"]

    def summary(self) -> dict[str, int]:
        statuses = [r.status for r in self.results]
        return {
            "total": len(statuses),
            "passed": statuses.count("PASS"),
            "warned": statuses.count("WARN"),
            "failed": statuses.count("FAIL"),
        }

    def to_report(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "mode": self.mode,
            "summary": self.summary(),
            "results": [{**asdict(r), "status": r.status} for r in self.results],
        }

    def write(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_report(), indent=2, default=str))
        return path

    def enforce(self, stage: str = "") -> None:
        if self.mode == "fail" and self.failed:
            names = "; ".join(f"{r.table}.{r.check}" for r in self.failed)
            raise DataQualityError(f"{len(self.failed)} error-severity DQ check(s) failed {stage}: {names}")
