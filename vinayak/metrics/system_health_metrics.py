from __future__ import annotations

from datetime import timedelta
from typing import Any

import pandas as pd

from vinayak.metrics.utils import coerce_candle_records, coerce_health_snapshots, safe_divide, utc_now


def calculate_system_health_metrics(health_snapshots: Any, candles: Any = None, stale_threshold_minutes: int = 15) -> dict[str, Any]:
    health = coerce_health_snapshots(health_snapshots)
    candles_df = coerce_candle_records(candles) if candles is not None else pd.DataFrame(columns=['timestamp'])
    recent_errors: list[str] = []
    if not health.empty and 'error_message' in health.columns:
        recent_errors = [str(item) for item in health['error_message'].dropna().astype(str).tail(5).tolist() if str(item).strip()]

    latest_candle_ts = candles_df['timestamp'].max() if not candles_df.empty else pd.NaT
    reference_time = health['timestamp'].max() if not health.empty else utc_now()
    stale_detected = False
    if pd.notna(latest_candle_ts):
        stale_detected = latest_candle_ts < (reference_time - timedelta(minutes=int(stale_threshold_minutes)))

    def _rate(column: str) -> float:
        if health.empty or column not in health.columns:
            return 0.0
        valid = health[column].dropna()
        return round(float(valid.astype(bool).mean()) if not valid.empty else 0.0, 4)

    return {
        'average_data_latency_ms': round(float(pd.to_numeric(health.get('data_latency_ms', pd.Series(dtype=float)), errors='coerce').dropna().mean()) if not health.empty else 0.0, 4),
        'max_data_latency_ms': round(float(pd.to_numeric(health.get('data_latency_ms', pd.Series(dtype=float)), errors='coerce').dropna().max()) if not health.empty and not pd.to_numeric(health.get('data_latency_ms', pd.Series(dtype=float)), errors='coerce').dropna().empty else 0.0, 4),
        'average_api_latency_ms': round(float(pd.to_numeric(health.get('api_latency_ms', pd.Series(dtype=float)), errors='coerce').dropna().mean()) if not health.empty else 0.0, 4),
        'max_api_latency_ms': round(float(pd.to_numeric(health.get('api_latency_ms', pd.Series(dtype=float)), errors='coerce').dropna().max()) if not health.empty and not pd.to_numeric(health.get('api_latency_ms', pd.Series(dtype=float)), errors='coerce').dropna().empty else 0.0, 4),
        'signal_generation_success_rate': _rate('signal_generation_success'),
        'execution_service_success_rate': _rate('execution_success'),
        'broker_health_rate': _rate('broker_ok'),
        'telegram_health_rate': _rate('telegram_ok'),
        'pipeline_health_rate': _rate('pipeline_ok'),
        'error_count': int(len(recent_errors)),
        'recent_error_messages': recent_errors,
        'stale_data_detected': bool(stale_detected),
        'stale_data_events_count': int(1 if stale_detected else 0),
        'latest_candle_timestamp': latest_candle_ts.isoformat() if pd.notna(latest_candle_ts) else '',
    }

