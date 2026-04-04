"""
helpers/logger.py
=================
Centralised logging configuration for the data platform.

Why centralise logging?
  - Consistent format across all modules
  - Single place to change log level or format
  - Easy to swap to CloudWatch, Datadog, etc.
  - Avoids duplicate handlers in AWS Lambda

Usage:
    from helpers.logger import get_logger

    logger = get_logger(__name__)
    logger.info("Pipeline started")
    logger.warning("Missing field — skipping record")
    logger.error("Failed to connect to Redshift")

Author: Biswajit Praharaj
GitHub: github.com/Biswajit107927
"""

import logging
import os
import sys
from typing import Optional

# ── Log Level from Environment ────────────────────────────────────────────────
_DEFAULT_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# ── Log Format ────────────────────────────────────────────────────────────────
_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# ── Root Logger Setup ─────────────────────────────────────────────────────────
_configured = False


def _configure_root_logger():
    """Configure root logger once — avoids duplicate handlers in Lambda."""
    global _configured
    if _configured:
        return

    root = logging.getLogger()
    root.setLevel(getattr(logging, _DEFAULT_LEVEL, logging.INFO))

    # Remove existing handlers (important for Lambda re-use)
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Add stdout handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(handler)

    _configured = True


def get_logger(
    name: str,
    level: Optional[str] = None
) -> logging.Logger:
    """
    Get a configured logger for the given module name.

    Args:
        name: Logger name — use __name__ for module-level loggers
        level: Optional log level override (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Configured Logger instance

    Example:
        logger = get_logger(__name__)
        logger.info("Starting pipeline")
    """
    _configure_root_logger()

    logger = logging.getLogger(name)

    if level:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    return logger
