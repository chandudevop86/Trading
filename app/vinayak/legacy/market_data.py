"""Named adapter boundary for legacy market-data integrations."""

from __future__ import annotations

try:
    from src.live_ohlcv import fetch_live_ohlcv as fetch_legacy_live_ohlcv
except Exception:  # pragma: no cover
    fetch_legacy_live_ohlcv = None  # type: ignore

try:
    from src.dhan_api import load_security_map as load_legacy_security_map
except Exception:  # pragma: no cover
    load_legacy_security_map = None  # type: ignore

__all__ = ["fetch_legacy_live_ohlcv", "load_legacy_security_map"]
