"""Bronze layer: raw JSON-lines ingestion with schema enforcement and quarantine.

* Each landing file is ingested at most once (tracked in an ingestion log keyed by
  file name), so re-running the pipeline over the same landing zone is a no-op.
* Records are parsed against the entity contract. Malformed JSON, type mismatches,
  missing required fields, unknown ``op`` codes and unparseable timestamps go to a
  quarantine table together with the raw line and a reason.
* Every row carries ingestion metadata: ``_source_file``, ``_ingest_run_id``,
  ``_ingested_at``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse import storage
from lakehouse.config import PipelineConfig
from lakehouse.log import get_logger
from lakehouse.schemas import CORRUPT_COL, VALID_OPS, EntitySpec, parse_event_ts

log = get_logger(__name__)

META_COLS = ("_source_file", "_ingest_run_id", "_ingested_at")


@dataclass
class BronzeResult:
    entity: str
    files_ingested: list[str] = field(default_factory=list)
    files_skipped: int = 0
    records_read: int = 0
    records_valid: int = 0
    records_quarantined: int = 0

    @property
    def quarantine_ratio(self) -> float:
        return self.records_quarantined / self.records_read if self.records_read else 0.0


def bronze_path(cfg: PipelineConfig, entity: str) -> Path:
    return cfg.bronze_dir / entity


def quarantine_path(cfg: PipelineConfig, entity: str) -> Path:
    return cfg.quarantine_dir / entity


def _ingest_log_path(cfg: PipelineConfig, entity: str) -> Path:
    return cfg.checkpoint_dir / f"bronze_{entity}_ingest_log.json"


def parse_records(raw: DataFrame, spec: EntitySpec) -> DataFrame:
    """Parse a ``value`` column of JSON strings and attach a ``_quarantine_reason``.

    Pure function (no I/O) so it can be unit tested directly.
    """
    parsed = raw.withColumn(
        "_rec",
        F.from_json(
            "value",
            spec.schema_with_corrupt,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COL},
        ),
    )
    missing = [F.when(F.col(f"_rec.{c}").isNull(), F.lit(c)) for c in spec.required]
    checks = [
        (F.col("_rec").isNull() | F.col(f"_rec.{CORRUPT_COL}").isNotNull(), F.lit("malformed_json_or_type_mismatch")),
        (F.concat_ws(",", *missing) != "", F.concat(F.lit("missing_required:"), F.concat_ws(",", *missing))),
    ]
    if spec.has_op:
        checks.append((~F.upper(F.col("_rec.op")).isin(*VALID_OPS), F.lit("invalid_op")))
    checks.append((parse_event_ts(F.col(f"_rec.{spec.ts_field}")).isNull(), F.lit("invalid_timestamp")))

    reason = None
    for cond, value in checks:  # first matching rule wins
        reason = F.when(cond, value) if reason is None else reason.when(cond, value)
    return parsed.withColumn("_quarantine_reason", reason)


def ingest_entity(spark: SparkSession, cfg: PipelineConfig, spec: EntitySpec, run_id: str) -> BronzeResult:
    result = BronzeResult(entity=spec.name)
    landing = cfg.landing_dir / spec.name
    all_files = sorted(p for p in landing.glob("*.jsonl")) if landing.exists() else []
    ingest_log: dict = storage.load_json(_ingest_log_path(cfg, spec.name), {})
    new_files = [p for p in all_files if p.name not in ingest_log]
    result.files_skipped = len(all_files) - len(new_files)
    if not new_files:
        log.info("bronze.%s: no new files (%d already ingested)", spec.name, result.files_skipped)
        return result

    raw = (
        spark.read.text([str(p) for p in new_files])
        .withColumn("_source_file", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1))
        .filter(F.trim("value") != "")
    )
    parsed = (
        parse_records(raw, spec)
        .withColumn("_ingest_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.lit(datetime.now(UTC)).cast("timestamp"))
        .cache()
    )

    valid = parsed.filter(F.col("_quarantine_reason").isNull()).select(
        *[F.col(f"_rec.{f.name}").alias(f.name) for f in spec.schema.fields], *META_COLS
    )
    if spec.has_op:
        valid = valid.withColumn("op", F.upper("op"))
    quarantined = parsed.filter(F.col("_quarantine_reason").isNotNull()).select(
        F.lit(spec.name).alias("entity"), F.col("value").alias("_raw"), "_quarantine_reason", *META_COLS
    )

    storage.append_table(valid, bronze_path(cfg, spec.name), cfg.storage_format)
    storage.append_table(quarantined, quarantine_path(cfg, spec.name), cfg.storage_format)

    per_file = {r["_source_file"]: r for r in parsed.groupBy("_source_file").agg(
        F.count("*").alias("read"),
        F.sum(F.when(F.col("_quarantine_reason").isNull(), 1).otherwise(0)).alias("valid"),
    ).collect()}  # fmt: skip
    parsed.unpersist()

    # Log is written only after both appends succeed. A crash in between could
    # re-append a file on retry; silver deduplicates by event/txn id, so that
    # remains harmless (defence in depth).
    for p in new_files:
        stats = per_file.get(p.name)
        ingest_log[p.name] = {
            "run_id": run_id,
            "read": int(stats["read"]) if stats else 0,
            "valid": int(stats["valid"]) if stats else 0,
        }
        result.records_read += ingest_log[p.name]["read"]
        result.records_valid += ingest_log[p.name]["valid"]
    storage.save_json(_ingest_log_path(cfg, spec.name), ingest_log)
    result.files_ingested = [p.name for p in new_files]
    result.records_quarantined = result.records_read - result.records_valid
    log.info(
        "bronze.%s: files=%d read=%d valid=%d quarantined=%d",
        spec.name, len(new_files), result.records_read, result.records_valid, result.records_quarantined,
    )  # fmt: skip
    return result
