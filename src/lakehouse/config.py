"""Pipeline configuration loaded from TOML with programmatic overrides."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

SUPPORTED_FORMATS = ("parquet", "delta")
DQ_MODES = ("fail", "warn")


@dataclass(frozen=True)
class PipelineConfig:
    """All knobs the pipeline needs. Paths are local filesystem paths."""

    base_dir: Path = Path("data/lakehouse")
    landing_dir: Path = Path("data/landing")
    storage_format: str = "parquet"
    spark_master: str = "local[2]"
    shuffle_partitions: int = 4
    dq_mode: str = "fail"
    max_quarantine_ratio: float = 0.02
    log_level: str = "INFO"
    extra_spark_conf: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.storage_format not in SUPPORTED_FORMATS:
            raise ValueError(f"storage_format must be one of {SUPPORTED_FORMATS}")
        if self.dq_mode not in DQ_MODES:
            raise ValueError(f"dq_mode must be one of {DQ_MODES}")
        object.__setattr__(self, "base_dir", Path(self.base_dir))
        object.__setattr__(self, "landing_dir", Path(self.landing_dir))

    # Layer locations -----------------------------------------------------
    @property
    def bronze_dir(self) -> Path:
        return self.base_dir / "bronze"

    @property
    def quarantine_dir(self) -> Path:
        return self.base_dir / "bronze" / "_quarantine"

    @property
    def silver_dir(self) -> Path:
        return self.base_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.base_dir / "gold"

    @property
    def checkpoint_dir(self) -> Path:
        return self.base_dir / "_checkpoints"

    @property
    def reports_dir(self) -> Path:
        return self.base_dir / "_reports"

    def with_overrides(self, **overrides: Any) -> PipelineConfig:
        clean = {k: v for k, v in overrides.items() if v is not None}
        return replace(self, **clean)

    @classmethod
    def from_toml(cls, path: str | Path, **overrides: Any) -> PipelineConfig:
        with open(path, "rb") as fh:
            raw = tomllib.load(fh)
        paths = raw.get("paths", {})
        spark = raw.get("spark", {})
        storage = raw.get("storage", {})
        dq = raw.get("data_quality", {})
        cfg = cls(
            base_dir=Path(paths.get("base_dir", cls.base_dir)),
            landing_dir=Path(paths.get("landing_dir", cls.landing_dir)),
            storage_format=storage.get("format", cls.storage_format),
            spark_master=spark.get("master", cls.spark_master),
            shuffle_partitions=int(spark.get("shuffle_partitions", cls.shuffle_partitions)),
            extra_spark_conf={str(k): str(v) for k, v in spark.get("conf", {}).items()},
            dq_mode=dq.get("mode", cls.dq_mode),
            max_quarantine_ratio=float(dq.get("max_quarantine_ratio", cls.max_quarantine_ratio)),
            log_level=raw.get("log_level", cls.log_level),
        )
        return cfg.with_overrides(**overrides)
