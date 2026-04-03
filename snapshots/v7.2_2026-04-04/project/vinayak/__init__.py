"""Compatibility shim that maps the legacy ``vinayak`` import path to ``app/vinayak``."""

from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "app" / "vinayak"

__path__ = [str(_PACKAGE_ROOT)]
