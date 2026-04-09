"""Named adapter boundary for legacy option-chain integrations."""

from __future__ import annotations

try:
    from vinayak.data.nse_option_chain import build_metrics_map as build_legacy_option_metrics_map
    from vinayak.data.nse_option_chain import extract_option_records as extract_legacy_option_records
    from vinayak.data.nse_option_chain import fetch_option_chain as fetch_legacy_option_chain
    from vinayak.data.nse_option_chain import normalize_index_symbol as normalize_legacy_index_symbol
except ModuleNotFoundError:
    try:
        from src.nse_option_chain import build_metrics_map as build_legacy_option_metrics_map
        from src.nse_option_chain import extract_option_records as extract_legacy_option_records
        from src.nse_option_chain import fetch_option_chain as fetch_legacy_option_chain
        from src.nse_option_chain import normalize_index_symbol as normalize_legacy_index_symbol
    except Exception:  # pragma: no cover
        build_legacy_option_metrics_map = None  # type: ignore
        extract_legacy_option_records = None  # type: ignore
        fetch_legacy_option_chain = None  # type: ignore
        normalize_legacy_index_symbol = None  # type: ignore

__all__ = [
    "build_legacy_option_metrics_map",
    "extract_legacy_option_records",
    "fetch_legacy_option_chain",
    "normalize_legacy_index_symbol",
]
