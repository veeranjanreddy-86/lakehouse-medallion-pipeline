"""Logging setup shared by the CLI entry points."""

from __future__ import annotations

import logging

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s - %(message)s"


def setup_logging(level: str = "INFO") -> None:
    """Configure root logging once and quieten noisy JVM bridge loggers."""
    logging.basicConfig(level=level.upper(), format=_FORMAT, force=True)
    for noisy in ("py4j", "py4j.clientserver", "py4j.java_gateway"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
