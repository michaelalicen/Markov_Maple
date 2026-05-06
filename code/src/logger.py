"""
logger.py  —  VWS roster pipeline logging

Single source of truth for writing diagnostic output to the log file.
Nothing in Scheduler.py or OutputFormatter.py should call print() directly.
All output goes through log_print(), which writes to the log file only.

The log file path is set once by Main.py via setup_logger() before
build_and_solve() is called.  If setup_logger() has not been called
(e.g. during unit tests), log_print() falls back to print() so nothing breaks.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

_log_file = None   # open file handle, set by setup_logger()


def setup_logger(log_path: Path) -> None:
    """Open the log file for writing.  Call this once from Main.py."""
    global _log_file
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _log_file = log_path.open("w", encoding="utf-8", buffering=1)


def close_logger() -> None:
    """Flush and close the log file.  Call this from Main.py after the pipeline."""
    global _log_file
    if _log_file is not None:
        _log_file.flush()
        _log_file.close()
        _log_file = None


def log_print(*args, sep: str = " ", end: str = "\n") -> None:
    """Write a line to the log file (not the terminal).

    Signature mirrors print() so existing call sites need minimal changes.
    Falls back to print() if the logger has not been set up yet.
    """
    message = sep.join(str(a) for a in args) + end
    if _log_file is not None:
        _log_file.write(message)
        _log_file.flush()
    else:
        # Fallback: logger not initialised (e.g. running Scheduler standalone)
        sys.stdout.write(message)
