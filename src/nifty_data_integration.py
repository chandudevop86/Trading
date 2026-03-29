from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.data_processing import REQUIRED_OHLCV_COLUMNS, load_and_process_ohlcv
from src.live_ohlcv import fetch_live_ohlcv

CANONICAL_MARKET_DATA_COLUMNS = list(REQUIRED_OHLCV_COLUMNS)
STRICT_NIFTY_PROVIDER_ORDER = ('YAHOO', 'DHAN')
NIFTY_INDEX_SYMBOLS = {
    'NIFTY',
    'NIFTY50',
    'NIFTY 50',
    '^NSEI',
    'BANKNIFTY',
    'NIFTY BANK',
    '^NSEBANK',
    'FINNIFTY',
    'MIDCPNIFTY',
}
STRICT_MAX_INVALID_INTERVAL_RATIO = 0.05
STRICT_MAX_GAP_RATIO = 0.20
STRICT_MAX_ZERO_VOLUME_RATIO = 0.20
STRICT_MAX_ZERO_RANGE_RATIO = 0.10
STRICT_MAX_ABNORMAL_RETURN_PCT = 15.0
CRITICAL_DERIVED_COLUMNS = ['vwap', 'atr_14', 'atr_pct', 'range', 'interval_minutes']
STRICT_FRESHNESS_INTERVAL_MULTIPLIER = 3
STRICT_MAX_MISALIGNED_TIMESTAMP_RATIO = 0.05
MARKET_TZ = 'Asia/Kolkata'
MARKET_OPEN_HHMM = '09:15'
MARKET_CLOSE_HHMM = '15:30'


@dataclass(frozen=True, slots=True)
class ProviderAttemptResult:
    provider: str
    passed: bool
    reason: str
    rows_fetched: int

    def to_dict(self) -> dict[str, Any]:
        return {
            'provider': self.provider,
            'passed': self.passed,
            'reason': self.reason,
            'rows_fetched': self.rows_fetched,
        }


@dataclass(frozen=True, slots=True)
class NiftyDataBundle:
    symbol: str
    interval: str
    period: str
    provider: str
    frame: pd.DataFrame
    validation_report: dict[str, Any]
    provider_attempts: tuple[ProviderAttemptResult, ...]
    raw_rows: tuple[dict[str, Any], ...]


class NiftyDataValidationError(ValueError):
    pass


def _clean_text(value: object) -> str:
    return str(value or '').strip()


def _normalize_symbol(symbol: str) -> str:
    text = _clean_text(symbol).upper()
    return 'NIFTY' if text in {'^NSEI', 'NSEI', 'NIFTY50', 'NIFTY 50'} else text


def _interval_minutes(interval: str) -> int:
    text = _clean_text(interval).lower()
    if text.endswith('min'):
        text = text[:-3]
    if text.endswith('m'):
        return max(int(float(text[:-1] or 0)), 0)
    if text.endswith('h'):
        return max(int(float(text[:-1] or 0)) * 60, 0)
    if text.endswith('d'):
        return max(int(float(text[:-1] or 0)) * 375, 0)
    try:
        return max(int(float(text)), 0)
    except ValueError:
        return 0


def _provider_sequence(provider: str | None) -> list[str]:
    selected = _clean_text(provider).upper() or 'AUTO'
    if selected == 'AUTO':
        return list(STRICT_NIFTY_PROVIDER_ORDER)
    return [selected]


def _session_violation_count(frame: pd.DataFrame) -> int:
    if frame.empty or 'time_block' not in frame.columns:
        return 0
    return int(frame['time_block'].isin({'offhours', 'unknown'}).sum())


def _future_timestamp_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    now = pd.Timestamp.now().tz_localize(None)
    return int((frame['timestamp'] > now).sum())


def _all_zero_volume(frame: pd.DataFrame) -> bool:
    if frame.empty:
        return True
    return bool((pd.to_numeric(frame['volume'], errors='coerce').fillna(0.0) <= 0).all())


def _ratio(count: int, total: int) -> float:
    return float(count) / max(int(total), 1)


def _non_positive_price_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    price_columns = ['open', 'high', 'low', 'close']
    return int((frame[price_columns] <= 0).any(axis=1).sum())


def _critical_nan_columns(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return []
    missing: list[str] = []
    for column in CRITICAL_DERIVED_COLUMNS:
        if column not in frame.columns:
            missing.append(column)
            continue
        if frame[column].isna().any():
            missing.append(column)
    return missing


def _zero_volume_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    return int((pd.to_numeric(frame['volume'], errors='coerce').fillna(0.0) <= 0).sum())


def _zero_range_count(frame: pd.DataFrame) -> int:
    if frame.empty or 'range' not in frame.columns:
        return 0
    return int((pd.to_numeric(frame['range'], errors='coerce').fillna(0.0) <= 0).sum())


def _invalid_interval_count(frame: pd.DataFrame) -> int:
    if frame.empty or 'interval_valid' not in frame.columns:
        return 0
    return int((~frame['interval_valid'].fillna(False)).sum())


def _gap_count(frame: pd.DataFrame) -> int:
    if frame.empty or 'gap_flag' not in frame.columns:
        return 0
    return int(frame['gap_flag'].fillna(False).sum())


def _abnormal_return_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    closes = pd.to_numeric(frame['close'], errors='coerce')
    pct_change = closes.pct_change().abs() * 100.0
    return int((pct_change > STRICT_MAX_ABNORMAL_RETURN_PCT).fillna(False).sum())


def _latest_timestamp(frame: pd.DataFrame) -> pd.Timestamp | None:
    if frame.empty or 'timestamp' not in frame.columns:
        return None
    latest = frame['timestamp'].max()
    return None if pd.isna(latest) else pd.Timestamp(latest)


def _default_max_staleness_minutes(interval: str) -> int:
    base = max(_interval_minutes(interval), 1)
    return max(base * STRICT_FRESHNESS_INTERVAL_MULTIPLIER, 5)


def _is_market_session_now() -> bool:
    now = pd.Timestamp.now(tz=MARKET_TZ)
    if int(now.weekday()) >= 5:
        return False
    hhmm = now.strftime('%H:%M')
    return MARKET_OPEN_HHMM <= hhmm <= MARKET_CLOSE_HHMM


def _misaligned_timestamp_count(frame: pd.DataFrame, interval: str) -> int:
    interval_minutes = _interval_minutes(interval)
    if frame.empty or interval_minutes <= 1 or 'timestamp' not in frame.columns:
        return 0
    misaligned = 0
    for timestamp in frame['timestamp']:
        ts = pd.Timestamp(timestamp)
        minutes_since_open = ((ts.hour * 60) + ts.minute) - ((9 * 60) + 15)
        if minutes_since_open < 0 or (minutes_since_open % interval_minutes) != 0:
            misaligned += 1
    return misaligned


def _quality_score(row_count: int, metrics: dict[str, float | int | bool | list[str]]) -> float:
    score = 10.0
    score -= min(float(metrics.get('zero_volume_ratio', 0.0)) * 10.0, 2.0)
    score -= min(float(metrics.get('invalid_interval_ratio', 0.0)) * 20.0, 2.5)
    score -= min(float(metrics.get('gap_ratio', 0.0)) * 10.0, 2.0)
    score -= min(float(metrics.get('zero_range_ratio', 0.0)) * 10.0, 1.5)
    score -= min((int(metrics.get('abnormal_return_count', 0)) / max(row_count, 1)) * 15.0, 2.5)
    score -= min((int(metrics.get('misaligned_timestamp_count', 0)) / max(row_count, 1)) * 15.0, 2.5)
    score -= 1.5 if bool(metrics.get('freshness_checked')) and not bool(metrics.get('freshness_passed')) else 0.0
    score -= min(len(metrics.get('critical_nan_columns', [])) * 1.5, 4.0)
    return round(max(score, 0.0), 2)


def _staleness_minutes(frame: pd.DataFrame) -> float | None:
    latest = _latest_timestamp(frame)
    if latest is None:
        return None
    now = pd.Timestamp.now().tz_localize(None)
    age_seconds = (now - latest).total_seconds()
    return round(age_seconds / 60.0, 2)


def _strict_validate_market_data(
    frame: pd.DataFrame,
    report: dict[str, Any],
    *,
    symbol: str,
    interval: str,
    require_volume: bool,
    require_market_hours: bool,
    min_rows: int,
    require_freshness: bool,
    max_staleness_minutes: int | None,
) -> dict[str, Any]:
    issues: list[str] = []
    warnings: list[str] = []

    if frame.empty:
        issues.append('no candles available after normalization')
    missing_columns = [column for column in CANONICAL_MARKET_DATA_COLUMNS if column not in frame.columns]
    if missing_columns:
        issues.append(f'missing canonical columns: {missing_columns}')

    row_count = int(len(frame))
    if row_count < int(min_rows):
        issues.append(f'insufficient candle count: {row_count} < {int(min_rows)}')
    if not frame.empty and not frame['timestamp'].is_monotonic_increasing:
        issues.append('timestamps are not strictly ascending')
    if not frame.empty and frame['timestamp'].duplicated().any():
        issues.append('duplicate timestamps remain after validation')
    if _future_timestamp_count(frame) > 0:
        issues.append('future timestamps detected')

    non_positive_price_count = _non_positive_price_count(frame)
    if non_positive_price_count > 0:
        issues.append('non-positive OHLC prices detected')

    critical_nan_columns = _critical_nan_columns(frame)
    if critical_nan_columns:
        issues.append(f'critical derived columns contain nulls: {critical_nan_columns}')

    if require_market_hours and _interval_minutes(interval) > 0 and _session_violation_count(frame) > 0:
        issues.append('offhours candles detected in intraday dataset')

    zero_volume_count = _zero_volume_count(frame)
    zero_volume_ratio = _ratio(zero_volume_count, row_count)
    if require_volume and _all_zero_volume(frame):
        issues.append('volume is entirely zero; VWAP and execution-quality checks are not safe')
    elif require_volume and zero_volume_ratio > STRICT_MAX_ZERO_VOLUME_RATIO:
        issues.append(f'too many zero-volume candles: {zero_volume_count}/{row_count}')

    invalid_interval_count = _invalid_interval_count(frame)
    invalid_interval_ratio = _ratio(invalid_interval_count, row_count)
    if invalid_interval_ratio > STRICT_MAX_INVALID_INTERVAL_RATIO:
        issues.append(f'interval integrity too weak: {invalid_interval_count}/{row_count} candles invalid')

    gap_count = _gap_count(frame)
    gap_ratio = _ratio(gap_count, row_count)
    if gap_ratio > STRICT_MAX_GAP_RATIO:
        issues.append(f'too many missing-candle gaps: {gap_count}/{row_count}')
    elif gap_count > 0:
        warnings.append(f'gap_candles={gap_count}')

    zero_range_count = _zero_range_count(frame)
    zero_range_ratio = _ratio(zero_range_count, row_count)
    if zero_range_ratio > STRICT_MAX_ZERO_RANGE_RATIO:
        issues.append(f'too many flat candles: {zero_range_count}/{row_count}')

    abnormal_return_count = _abnormal_return_count(frame)
    if abnormal_return_count > 0:
        issues.append(f'abnormal candle-to-candle jumps detected: {abnormal_return_count}')

    misaligned_timestamp_count = _misaligned_timestamp_count(frame, interval)
    misaligned_timestamp_ratio = _ratio(misaligned_timestamp_count, row_count)
    if misaligned_timestamp_ratio > STRICT_MAX_MISALIGNED_TIMESTAMP_RATIO:
        issues.append(f'timestamp alignment too weak: {misaligned_timestamp_count}/{row_count}')

    latest_timestamp = _latest_timestamp(frame)
    staleness_minutes = _staleness_minutes(frame)
    resolved_max_staleness = int(max_staleness_minutes if max_staleness_minutes is not None else _default_max_staleness_minutes(interval))
    freshness_checked = bool(require_freshness and _interval_minutes(interval) > 0)
    freshness_enforced = bool(freshness_checked and _is_market_session_now())
    freshness_passed = True
    if freshness_checked and not freshness_enforced:
        warnings.append('freshness_not_enforced_outside_market_session')
    if freshness_enforced:
        if latest_timestamp is None or staleness_minutes is None:
            freshness_passed = False
            issues.append('latest candle timestamp is unavailable for freshness validation')
        elif staleness_minutes > float(resolved_max_staleness):
            freshness_passed = False
            issues.append(f'stale intraday feed: last candle age {staleness_minutes:.2f}m exceeds {resolved_max_staleness}m')

    if int(report.get('duplicates_removed', 0)) > 0:
        warnings.append(f"duplicates_removed={int(report.get('duplicates_removed', 0))}")
    if int(report.get('invalid_rows_removed', 0)) > 0:
        warnings.append(f"invalid_rows_removed={int(report.get('invalid_rows_removed', 0))}")
    if int(report.get('missing_rows_removed', 0)) > 0:
        warnings.append(f"missing_rows_removed={int(report.get('missing_rows_removed', 0))}")
    for warning in report.get('interval_warnings', []) or []:
        warnings.append(str(warning))

    return {
        'symbol': _normalize_symbol(symbol),
        'interval': interval,
        'rows_in': int(report.get('rows_in', 0)),
        'rows_out': row_count,
        'final_columns': list(frame.columns),
        'issues': issues,
        'warnings': warnings,
        'passed': not issues,
        'non_positive_price_count': non_positive_price_count,
        'zero_volume_count': zero_volume_count,
        'zero_volume_ratio': round(zero_volume_ratio, 4),
        'invalid_interval_count': invalid_interval_count,
        'invalid_interval_ratio': round(invalid_interval_ratio, 4),
        'gap_count': gap_count,
        'gap_ratio': round(gap_ratio, 4),
        'zero_range_count': zero_range_count,
        'zero_range_ratio': round(zero_range_ratio, 4),
        'abnormal_return_count': abnormal_return_count,
        'critical_nan_columns': critical_nan_columns,
        'misaligned_timestamp_count': misaligned_timestamp_count,
        'misaligned_timestamp_ratio': round(misaligned_timestamp_ratio, 4),
        'latest_timestamp': latest_timestamp.strftime('%Y-%m-%d %H:%M:%S') if latest_timestamp is not None else '',
        'last_candle_age_minutes': staleness_minutes,
        'freshness_checked': freshness_checked,
        'freshness_enforced': freshness_enforced,
        'freshness_passed': freshness_passed,
        'max_staleness_minutes': resolved_max_staleness if freshness_checked else None,
        'data_quality_score': _quality_score(
            row_count,
            {
                'zero_volume_ratio': zero_volume_ratio,
                'invalid_interval_ratio': invalid_interval_ratio,
                'gap_ratio': gap_ratio,
                'zero_range_ratio': zero_range_ratio,
                'abnormal_return_count': abnormal_return_count,
                'misaligned_timestamp_count': misaligned_timestamp_count,
                'freshness_checked': freshness_checked,
                'freshness_passed': freshness_passed,
                'critical_nan_columns': critical_nan_columns,
            },
        ),
    }


def fetch_nifty_data_bundle(
    symbol: str,
    interval: str = '5m',
    period: str = '5d',
    *,
    provider: str | None = 'AUTO',
    security_map: dict[str, Any] | None = None,
    broker_client: object | None = None,
    use_cache: bool = True,
    force_refresh: bool = False,
    require_volume: bool = True,
    require_market_hours: bool = True,
    require_freshness: bool = False,
    max_staleness_minutes: int | None = None,
    min_rows: int | None = None,
) -> NiftyDataBundle:
    normalized_symbol = _normalize_symbol(symbol)
    expected_interval_minutes = max(_interval_minutes(interval), 1)
    resolved_min_rows = int(min_rows if min_rows is not None else (20 if expected_interval_minutes <= 15 else 5))

    attempts: list[ProviderAttemptResult] = []
    last_raw_rows: list[dict[str, Any]] = []

    for provider_name in _provider_sequence(provider):
        rows = fetch_live_ohlcv(
            normalized_symbol,
            interval,
            period,
            provider=provider_name,
            security_map=security_map,
            broker_client=broker_client,
            use_cache=use_cache,
            force_refresh=force_refresh,
        )
        last_raw_rows = [dict(row) for row in rows or []]
        if not last_raw_rows:
            attempts.append(ProviderAttemptResult(provider=provider_name, passed=False, reason='provider returned no rows', rows_fetched=0))
            continue

        frame, processing_report = load_and_process_ohlcv(
            last_raw_rows,
            include_derived=True,
            expected_interval_minutes=expected_interval_minutes,
        )
        validation_report = _strict_validate_market_data(
            frame,
            processing_report,
            symbol=normalized_symbol,
            interval=interval,
            require_volume=require_volume,
            require_market_hours=require_market_hours,
            min_rows=resolved_min_rows,
            require_freshness=require_freshness,
            max_staleness_minutes=max_staleness_minutes,
        )
        merged_report = {**processing_report, **validation_report}
        if validation_report['passed']:
            attempts.append(ProviderAttemptResult(provider=provider_name, passed=True, reason='validation_passed', rows_fetched=len(last_raw_rows)))
            return NiftyDataBundle(
                symbol=normalized_symbol,
                interval=interval,
                period=period,
                provider=provider_name,
                frame=frame,
                validation_report=merged_report,
                provider_attempts=tuple(attempts),
                raw_rows=tuple(last_raw_rows),
            )

        attempts.append(
            ProviderAttemptResult(
                provider=provider_name,
                passed=False,
                reason='; '.join(validation_report['issues']) or 'validation_failed',
                rows_fetched=len(last_raw_rows),
            )
        )

    provider_messages = ', '.join(f"{attempt.provider}:{attempt.reason}" for attempt in attempts) or 'no provider attempts executed'
    raise NiftyDataValidationError(
        f'Strict real-market-data validation failed for {normalized_symbol} {interval} {period}. {provider_messages}'
    )


def fetch_nifty_ohlcv_frame(
    symbol: str,
    interval: str = '5m',
    period: str = '5d',
    **kwargs: Any,
) -> pd.DataFrame:
    bundle = fetch_nifty_data_bundle(symbol, interval=interval, period=period, **kwargs)
    return bundle.frame.copy()


__all__ = [
    'CANONICAL_MARKET_DATA_COLUMNS',
    'NiftyDataBundle',
    'NiftyDataValidationError',
    'ProviderAttemptResult',
    'STRICT_NIFTY_PROVIDER_ORDER',
    'fetch_nifty_data_bundle',
    'fetch_nifty_ohlcv_frame',
]












