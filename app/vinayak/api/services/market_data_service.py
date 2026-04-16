from __future__ import annotations

"""Market-data services for the live trading workspace."""

from datetime import UTC, datetime
from typing import Any, Callable

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore


MARKET_HEARTBEAT_MIN_REFRESH_SECONDS = 300
MARKET_HEARTBEAT_MAX_ROWS = 120


def prepare_trading_data(
    df: "pd.DataFrame",
    *,
    canonical_prepare_trading_data_fn: Callable[..., "pd.DataFrame"],
) -> "pd.DataFrame":
    if df is None or df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    return canonical_prepare_trading_data_fn(df, include_derived=False)


def df_to_candles(df: "pd.DataFrame", *, candle_cls: type[Any]) -> list[Any]:
    candles: list[Any] = []
    for row in df.itertuples(index=False):
        ts = getattr(row, "timestamp", None)
        if ts is None:
            continue
        if isinstance(ts, pd.Timestamp):
            ts_dt = ts.to_pydatetime()
        else:
            ts_dt = pd.to_datetime(ts, errors="coerce")
            if pd.isna(ts_dt):
                continue
            ts_dt = ts_dt.to_pydatetime()
        candles.append(
            candle_cls(
                timestamp=ts_dt,
                open=float(getattr(row, "open", 0.0) or 0.0),
                high=float(getattr(row, "high", 0.0) or 0.0),
                low=float(getattr(row, "low", 0.0) or 0.0),
                close=float(getattr(row, "close", 0.0) or 0.0),
                volume=float(getattr(row, "volume", 0.0) or 0.0),
            )
        )
    candles.sort(key=lambda item: item.timestamp)
    return candles


def data_status(candles: "pd.DataFrame") -> dict[str, Any]:
    report = dict(getattr(candles, "attrs", {}).get("cleaning_report", {}) or {})
    return {
        "status": "VALID" if not candles.empty else "INVALID",
        "rows": int(len(candles)),
        "latest_timestamp": report.get("latest_timestamp", str(candles.iloc[-1]["timestamp"]) if not candles.empty else ""),
        "duplicates_removed": int(report.get("duplicates_removed", 0) or 0),
        "columns": list(report.get("columns", list(candles.columns))),
    }


def parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = pd.to_datetime(text, errors="coerce", utc=True)
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def recent_market_snapshot(
    interval: str,
    *,
    get_observability_snapshot_fn: Callable[[], dict[str, Any]],
    max_age_seconds: int = MARKET_HEARTBEAT_MIN_REFRESH_SECONDS,
) -> dict[str, Any] | None:
    snapshot = get_observability_snapshot_fn()
    metrics = dict(snapshot.get("metrics", {}) or {})
    latest_metric = dict(metrics.get("latest_data_timestamp", {}) or {})
    latest_timestamp = str(latest_metric.get("value", "") or "").strip()
    if not latest_timestamp:
        return None

    market_data_delay = float(metrics.get("market_data_delay_seconds", {}).get("value", 0.0) or 0.0)
    updated_at = parse_iso_datetime(latest_metric.get("updated_at"))
    is_recent = market_data_delay <= float(max_age_seconds)
    if not is_recent and updated_at is not None:
        is_recent = (datetime.now(UTC) - updated_at).total_seconds() <= max_age_seconds
    if not is_recent:
        return None

    rows_loaded = metrics.get("market_data_rows_loaded_total", {}).get("value", 0)
    return {
        "candles": [],
        "candle_count": 0,
        "data_status": {
            "status": "VALID",
            "rows": int(rows_loaded or 0),
            "latest_timestamp": latest_timestamp,
            "duplicates_removed": int(metrics.get("market_data_duplicates_total", {}).get("value", 0) or 0),
            "provider": "",
            "source": "OBSERVABILITY_CACHE",
            "latest_interval": interval,
            "market_data_delay_seconds": market_data_delay,
            "refresh_mode": "CACHE_HIT",
        },
    }


def update_observability_metrics_from_run(
    rows: list[dict[str, Any]],
    candles: "pd.DataFrame",
    *,
    run_full_metrics_engine_fn: Callable[..., dict[str, Any]],
    set_metric_fn: Callable[[str, Any], None],
) -> None:
    if candles is not None and not candles.empty:
        latest_timestamp = str(candles.iloc[-1]["timestamp"])
        set_metric_fn("latest_data_timestamp", latest_timestamp)
        set_metric_fn("market_data_rows_loaded_total", int(len(candles)))
        latest_dt = pd.to_datetime(candles.iloc[-1]["timestamp"], errors="coerce", utc=True)
        if not pd.isna(latest_dt):
            delay_seconds = max(0.0, (datetime.now(UTC) - latest_dt.to_pydatetime()).total_seconds())
            set_metric_fn("market_data_delay_seconds", round(delay_seconds, 2))
        cleaning_report = dict(getattr(candles, "attrs", {}).get("cleaning_report", {}) or {})
        set_metric_fn("market_data_duplicates_total", int(cleaning_report.get("duplicates_removed", 0) or 0))
        set_metric_fn("market_data_nulls_total", int(cleaning_report.get("null_rows_removed", 0) or 0))
        set_metric_fn("schema_validation_failures_total", int(len(cleaning_report.get("issues", []) or [])))
    if not rows:
        set_metric_fn("rolling_win_rate", 0.0)
        set_metric_fn("rolling_expectancy", 0.0)
        set_metric_fn("pnl_today", 0.0)
        return
    metrics = run_full_metrics_engine_fn(rows, candles=candles)
    performance = dict(metrics.get("performance", {}) or {})
    execution = dict(metrics.get("execution", {}) or {})
    validation = dict(metrics.get("validation", {}) or {})
    set_metric_fn("rolling_win_rate", round(float(performance.get("win_rate", 0.0)) * 100.0, 2))
    set_metric_fn("rolling_expectancy", round(float(performance.get("expectancy", 0.0)), 4))
    set_metric_fn("pnl_today", round(float(performance.get("net_profit", 0.0)), 2))
    set_metric_fn("execution_success_rate", round(float(execution.get("execution_success_rate", 0.0)), 4))
    set_metric_fn("validation_pass_rate", round(float(validation.get("validation_pass_rate", 0.0)), 4))
    set_metric_fn("high_quality_setup_rate", round(float(validation.get("high_quality_setup_rate", 0.0)), 4))


def refresh_market_data_snapshot(
    *,
    symbol: str,
    interval: str,
    period: str,
    security_map_path: str,
    fetch_live_ohlcv_fn: Callable[..., list[dict[str, Any]]],
    prepare_trading_data_fn: Callable[["pd.DataFrame"], "pd.DataFrame"],
    normalize_rows_fn: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    recent_market_snapshot_fn: Callable[[str], dict[str, Any] | None],
    update_observability_metrics_from_run_fn: Callable[[list[dict[str, Any]], "pd.DataFrame"], None],
    data_status_fn: Callable[["pd.DataFrame"], dict[str, Any]],
) -> dict[str, Any]:
    recent_snapshot = recent_market_snapshot_fn(interval)
    if recent_snapshot is not None:
        return {
            "symbol": symbol,
            "interval": interval,
            "period": period,
            **recent_snapshot,
        }

    live_rows = fetch_live_ohlcv_fn(
        symbol=symbol,
        interval=interval,
        period=period,
        provider="DHAN",
        security_map_path=security_map_path,
        force_refresh=False,
    )
    live_rows = list(live_rows[-MARKET_HEARTBEAT_MAX_ROWS:])
    candles_df = prepare_trading_data_fn(pd.DataFrame(live_rows))
    update_observability_metrics_from_run_fn([], candles_df)
    normalized_rows = normalize_rows_fn(live_rows)
    latest_row = live_rows[-1] if live_rows else {}
    return {
        "symbol": symbol,
        "interval": interval,
        "period": period,
        "candles": normalized_rows,
        "candle_count": len(normalized_rows),
        "data_status": {
            **data_status_fn(candles_df),
            "provider": str(latest_row.get("provider", "") or ""),
            "source": str(latest_row.get("source", "") or ""),
            "latest_interval": str(latest_row.get("interval", interval) or interval),
            "refresh_mode": "FETCHED",
        },
    }

