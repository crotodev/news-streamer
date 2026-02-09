"""Central logging configuration for CLI and library usage."""

from __future__ import annotations

import logging
import os
import sys
import time


def configure_logging(log_level: str | None = None) -> None:
    """Configure root logging with a consistent formatter.

    Uses LOG_LEVEL env var (default INFO) when log_level is not provided.
    Safe to call multiple times without duplicating handlers.
    """
    level_name = (log_level or os.getenv("LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)

    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.setLevel(level)
        return

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)

    root_logger.setLevel(level)
    root_logger.addHandler(handler)
