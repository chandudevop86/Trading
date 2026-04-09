"""Compatibility shim that maps the legacy ``vinayak`` import path to ``app/vinayak``."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_PACKAGE_ROOT = _REPO_ROOT / "app" / "vinayak"

__path__ = [str(_PACKAGE_ROOT)]
