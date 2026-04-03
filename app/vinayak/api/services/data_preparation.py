from __future__ import annotations

from typing import Any

import pandas as pd

from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import increment_metric, record_stage, set_metric
from vinayak.data.cleaner import CleanerConfig, coerce_ohlcv
from src.data_processing import REQUIRED_OHLCV_COLUMNS, load_and_process_ohlcv


def enrich_trading_data(df: Any, *, expected_interval_minutes: int = 5) -> pd.DataFrame:
    try:
        cleaned = coerce_ohlcv(
        df,
        CleanerConfig(
            expected_interval_minutes=expected_interval_minutes,
            require_vwap=True,
            allow_vwap_compute=True,
        ),
    )
        prepared, _ = load_and_process_ohlcv(
            cleaned,
            include_derived=True,
            expected_interval_minutes=expected_interval_minutes,
        )
        prepared.attrs.update(dict(getattr(cleaned, 'attrs', {}) or {}))
        latest_ts = str(prepared.iloc[-1]['timestamp']) if not prepared.empty else ''
        set_metric('latest_data_timestamp', latest_ts)
        increment_metric('market_data_rows_loaded_total', len(prepared))
        set_metric('market_data_status', 'VALID' if not prepared.empty else 'INVALID')
        log_event(component='data_preparation', event_name='data_enriched', severity='INFO', message='Trading data enriched', context_json={'rows': len(prepared), 'latest_data_timestamp': latest_ts})
        return prepared
    except Exception as exc:
        increment_metric('schema_validation_failures_total', 1)
        record_stage('dataframe_normalize', status='FAIL', message=str(exc))
        log_exception(component='data_preparation', event_name='data_enrichment_failed', exc=exc, message='Trading data enrichment failed')
        raise


def prepare_trading_data(df: Any, *, include_derived: bool = False) -> pd.DataFrame:
    """Normalize OHLCV rows into Vinayak's canonical trading-data schema."""
    try:
        cleaned = coerce_ohlcv(
        df,
        CleanerConfig(
            require_vwap=bool(include_derived),
            allow_vwap_compute=bool(include_derived),
        ),
    )
        prepared, _ = load_and_process_ohlcv(cleaned, include_derived=include_derived)
        prepared.attrs.update(dict(getattr(cleaned, 'attrs', {}) or {}))
        latest_ts = str(prepared.iloc[-1]['timestamp']) if not prepared.empty else ''
        set_metric('latest_data_timestamp', latest_ts)
        set_metric('market_data_status', 'VALID' if not prepared.empty else 'INVALID')
        if latest_ts:
            delay = max(0.0, (pd.Timestamp.utcnow().tz_localize(None) - pd.to_datetime(latest_ts)).total_seconds())
            set_metric('market_data_delay_seconds', round(delay, 2))
        increment_metric('market_data_rows_loaded_total', len(prepared))
        nulls_total = int(prepared[['open', 'high', 'low', 'close', 'volume']].isna().sum().sum()) if not prepared.empty else 0
        increment_metric('market_data_nulls_total', nulls_total)
        duplicates_removed = int(dict(getattr(prepared, 'attrs', {}).get('cleaning_report', {}) or {}).get('duplicates_removed', 0) or 0)
        increment_metric('market_data_duplicates_total', duplicates_removed)
        record_stage('dataframe_normalize', status='SUCCESS', message='Trading data normalized')
        log_event(component='data_preparation', event_name='schema_validation', severity='INFO', message='OHLCV normalization passed', context_json={'rows': len(prepared), 'nulls': nulls_total, 'duplicates_removed': duplicates_removed, 'latest_data_timestamp': latest_ts})
        if include_derived:
            return prepared
        if prepared.empty:
            empty = pd.DataFrame(columns=REQUIRED_OHLCV_COLUMNS)
            empty.attrs.update(dict(getattr(cleaned, 'attrs', {}) or {}))
            return empty
        result = prepared.loc[:, REQUIRED_OHLCV_COLUMNS].copy()
        result.attrs.update(dict(getattr(prepared, 'attrs', {}) or {}))
        return result
    except Exception as exc:
        increment_metric('schema_validation_failures_total', 1)
        set_metric('market_data_status', 'INVALID')
        record_stage('dataframe_normalize', status='FAIL', message=str(exc))
        log_exception(component='data_preparation', event_name='schema_validation_failed', exc=exc, message='OHLCV normalization failed')
        raise


__all__ = ["REQUIRED_OHLCV_COLUMNS", "enrich_trading_data", "prepare_trading_data"]

