"""SparkSession factory. Delta Lake is enabled only when explicitly requested."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from lakehouse.config import PipelineConfig
from lakehouse.log import get_logger

log = get_logger(__name__)

_BASE_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.ui.enabled": "false",
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.driver.memory": "1g",
}


def build_spark(cfg: PipelineConfig, app_name: str = "lakehouse-medallion") -> SparkSession:
    """Create (or reuse) a SparkSession configured for this pipeline."""
    # Python workers must run the same interpreter as the driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(app_name).master(cfg.spark_master)
    conf = {**_BASE_CONF, "spark.sql.shuffle.partitions": str(cfg.shuffle_partitions)}
    conf.update(cfg.extra_spark_conf)
    for key, value in conf.items():
        builder = builder.config(key, value)

    if cfg.storage_format == "delta":
        builder = _with_delta(builder)
        try:
            spark = builder.getOrCreate()
        except Exception as exc:  # JVM fails to start when the Delta JARs cannot be resolved
            raise RuntimeError(
                "Could not start Spark with Delta Lake. delta-spark downloads its JARs from Maven "
                "Central on first use; if that is blocked, set LAKEHOUSE_SPARK_JARS to local "
                "delta-spark_2.12-3.2.1.jar and delta-storage-3.2.1.jar paths, or use --format parquet."
            ) from exc
    else:
        spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _with_delta(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Enable Delta Lake.

    By default delta-spark resolves its JARs from Maven Central via Ivy. In an
    offline/air-gapped environment set ``LAKEHOUSE_SPARK_JARS`` to a comma-separated
    list of local JAR paths (delta-spark_2.12 and delta-storage) instead.
    """
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    local_jars = os.environ.get("LAKEHOUSE_SPARK_JARS")
    if local_jars:
        log.info("Using pre-provisioned Delta JARs from LAKEHOUSE_SPARK_JARS")
        return builder.config("spark.jars", local_jars)
    try:
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise RuntimeError("storage_format='delta' requires `pip install delta-spark`") from exc
    return configure_spark_with_delta_pip(builder)
