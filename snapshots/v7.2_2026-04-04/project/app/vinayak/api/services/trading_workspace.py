from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from vinayak.api.services.data_preparation import prepare_trading_data as canonical_prepare_trading_data
from vinayak.api.services.strategy_workflow import Candle, StrategyContext, run_strategy_workflow
from vinayak.api.services.strike_selector import attach_option_strikes
from vinayak.analytics.readiness import evaluate_readiness
from vinayak.metrics import run_full_metrics_engine
from vinayak.notifications.telegram.service import build_trade_summary, send_telegram_message
from vinayak.validation.trade_evaluation import build_trade_evaluation_summary
from vinayak.observability.alerting import publish_active_alerts
from vinayak.observability.observability_logger import log_event, log_exception
from vinayak.observability.observability_metrics import get_observability_snapshot, record_stage, set_metric
from vinayak.api.services.live_ohlcv import fetch_live_ohlcv
from vinayak.execution.gateway import execute_workspace_candidates
from vinayak.api.services.report_storage import cache_json_artifact, store_json_report, store_text_report
from vinayak.messaging.bus import build_message_bus
from vinayak.messaging.topics import EVENT_ANALYSIS_COMPLETED, EVENT_NOTIFICATION_REQUESTED

try:
    from src.dhan_api import load_security_map
except Exception:  # pragma: no cover
    load_security_map = None  # type: ignore

try:
    from src.nse_option_chain import build_metrics_map, extract_option_records, fetch_option_chain, normalize_index_symbol
except Exception:  # pragma: no cover
    build_metrics_map = None  # type: ignore
    extract_option_records = None  # type: ignore
    fetch_option_chain = None  # type: ignore
    normalize_index_symbol = None  # type: ignore


if pd is None:  # pragma: no cover
    raise ModuleNotFoundError('pandas is required for trading workspace integration')


DEFAULT_PAPER_LOG_PATH = Path('vinayak/data/paper_trading_logs_all.csv')
DEFAULT_LIVE_LOG_PATH = Path('vinayak/data/live_trading_logs_all.csv')
MARKET_HEARTBEAT_MIN_REFRESH_SECONDS = 300
MARKET_HEARTBEAT_MAX_ROWS = 120

def _resolve_workspace_auto_execution_mode(requested_execution_type: str, auto_execute: bool) -> tuple[str, str]:
    requested = str(requested_execution_type or 'NONE').strip().upper()
    if not auto_execute:
        return requested, ''
    if requested == 'LIVE':
        return 'PAPER', 'Auto-execute forced to PAPER mode. Live entry and exit require an explicit manual route.'
    if requested == 'PAPER':
        return 'PAPER', 'Auto-execute is operating in PAPER mode.'
    return requested, ''


def prepare_trading_data(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    df = df.copy()
    if 'timestamp' not in df.columns:
        raise ValueError('Candles missing timestamp column')

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=['timestamp', 'open', 'high', 'low', 'close'])
    df = df.drop_duplicates(subset=['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    return df


def df_to_candles(df: pd.DataFrame) -> list[Candle]:
    candles: list[Candle] = []
    for row in df.itertuples(index=False):
        ts = getattr(row, 'timestamp', None)
        if ts is None:
            continue
        if isinstance(ts, pd.Timestamp):
            ts_dt = ts.to_pydatetime()
        else:
            ts_dt = pd.to_datetime(ts, errors='coerce')
            if pd.isna(ts_dt):
                continue
            ts_dt = ts_dt.to_pydatetime()
        candles.append(
            Candle(
                timestamp=ts_dt,
                open=float(getattr(row, 'open', 0.0) or 0.0),
                high=float(getattr(row, 'high', 0.0) or 0.0),
                low=float(getattr(row, 'low', 0.0) or 0.0),
                close=float(getattr(row, 'close', 0.0) or 0.0),
                volume=float(getattr(row, 'volume', 0.0) or 0.0),
            )
        )
    candles.sort(key=lambda item: item.timestamp)
    return candles


def _format_expiry(expiry: object) -> str:
    if expiry is None:
        return ''
    text = str(expiry).strip()
    if not text or text in {'-', 'N/A'}:
        return ''
    for fmt in ('%Y-%m-%d', '%d-%b-%Y'):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except Exception:
            pass
    return text


def _estimate_weekly_expiry(symbol: str, now: datetime | None = None) -> str:
    s = (symbol or '').strip().upper()
    if s in {'^NSEI', 'NIFTY', 'NIFTY 50', 'NIFTY50'}:
        dt = now or datetime.now(UTC) + timedelta(hours=5, minutes=30)
        days_ahead = (3 - dt.weekday()) % 7
        expiry = dt.date() + timedelta(days=days_ahead)
        return expiry.isoformat()
    return ''


def attach_indicator_trade_levels(rows: list[dict[str, object]], rr_ratio: float, trailing_sl_pct: float) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    sl_frac = max(0.0, float(trailing_sl_pct) / 100.0)
    if sl_frac <= 0:
        sl_frac = 0.002
    for item in rows:
        row = dict(item)
        side = str(row.get('side', '') or '').upper()
        try:
            entry = float(row.get('close', row.get('price', row.get('entry_price', 0.0))) or 0.0)
        except Exception:
            entry = 0.0
        if entry > 0 and side in {'BUY', 'SELL'}:
            row['entry_price'] = round(entry, 2)
            if side == 'BUY':
                sl = entry * (1.0 - sl_frac)
                tp = entry + (entry - sl) * float(rr_ratio)
            else:
                sl = entry * (1.0 + sl_frac)
                tp = entry - (sl - entry) * float(rr_ratio)
            row['stop_loss'] = round(sl, 2)
            row['target_price'] = round(tp, 2)
        if 'timestamp' in row:
            row['timestamp'] = str(row['timestamp'])
        out.append(row)
    return out


def attach_option_metrics(rows: list[dict[str, object]], symbol: str, fetch_option_metrics: bool) -> list[dict[str, object]]:
    if not rows:
        return rows

    metrics_map: dict[tuple[int, str], dict[str, object]] = {}
    status = 'DISABLED'
    if fetch_option_metrics and fetch_option_chain and extract_option_records and build_metrics_map and normalize_index_symbol:
        try:
            payload = fetch_option_chain(normalize_index_symbol(symbol), timeout=10.0)
            metrics_map = build_metrics_map(extract_option_records(payload))
            status = 'FETCH_OK'
        except Exception:
            metrics_map = {}
            status = 'FETCH_FAILED'

    enriched: list[dict[str, object]] = []
    any_nse_match = False
    any_estimated_expiry = False
    for item in rows:
        row = dict(item)
        strike_raw = row.get('strike_price', row.get('option_strike', ''))
        option_type = str(row.get('option_type', '') or '').upper()
        try:
            strike = int(float(strike_raw))
        except Exception:
            strike = 0

        metrics = metrics_map.get((strike, option_type), {}) if strike and option_type else {}
        if isinstance(metrics, dict) and metrics:
            row.update(metrics)
            any_nse_match = True
            if metrics.get('option_expiry'):
                row['option_expiry_source'] = 'NSE'

        if not row.get('option_expiry') and row.get('option_strike'):
            est = _estimate_weekly_expiry(symbol)
            if est:
                row['option_expiry'] = est
                row['option_expiry_source'] = 'ESTIMATED'
                any_estimated_expiry = True

        if row.get('option_expiry'):
            row['option_expiry'] = _format_expiry(row.get('option_expiry'))
        enriched.append(row)

    final_status = status
    if any_estimated_expiry and not any_nse_match:
        final_status = 'ESTIMATED_EXPIRY_ONLY'
    elif status == 'FETCH_OK' and not any_nse_match:
        final_status = 'NO_MATCH'
    elif status == 'FETCH_OK' and any_nse_match:
        final_status = 'NSE_OK'

    for row in enriched:
        row['_option_metrics_status'] = final_status
    return enriched


def attach_lots(rows: list[dict[str, object]], lot_size: int, lots: int) -> list[dict[str, object]]:
    lot_size = int(lot_size) if lot_size and int(lot_size) > 0 else 0
    lots = int(lots) if lots and int(lots) > 0 else 0
    if lot_size <= 0 or lots <= 0:
        return rows

    qty = lot_size * lots
    out: list[dict[str, object]] = []
    for item in rows:
        row = dict(item)
        row['lots'] = lots
        row['quantity'] = qty
        try:
            ltp = float(row.get('option_ltp', 0) or 0)
        except Exception:
            ltp = 0.0
        if ltp > 0:
            row['order_value'] = round(ltp * qty, 2)
        out.append(row)
    return out


def _normalize_rows(rows: list[dict[str, object]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in ['entry_price', 'stop_loss', 'target_price', 'spot_price', 'option_ltp', 'option_oi', 'option_vol', 'option_iv', 'order_value', 'price', 'share_price']:
            try:
                if key in item and item[key] is not None and str(item[key]).strip() != '':
                    item[key] = float(item[key])
            except Exception:
                pass
        for key in ['trade_no', 'strike_price', 'quantity', 'lots']:
            try:
                if key in item and item[key] is not None and str(item[key]).strip() != '':
                    item[key] = int(float(item[key]))
            except Exception:
                pass
        for key in ['timestamp', 'entry_time', 'signal_time', 'executed_at_utc', 'option_expiry']:
            if key in item and item[key] is not None:
                item[key] = str(item[key])
        normalized.append(item)
    return normalized


def _resolve_live_execution_kwargs(security_map_path: str) -> dict[str, object]:
    security_map: dict[str, dict[str, str]] = {}
    if load_security_map is not None:
        try:
            security_map = load_security_map(Path(str(security_map_path)))
        except Exception:
            security_map = {}
    return {'broker_name': 'DHAN', 'security_map': security_map}


def _build_report_artifacts(result: dict[str, Any]) -> dict[str, dict[str, str]]:
    trace_rows = result.get('execution_rows') or result.get('signals') or []
    summary_text = build_trade_summary(trace_rows) if trace_rows else 'No signals generated for this run.'
    json_artifact = store_json_report('live_analysis_result', result)
    summary_artifact = store_text_report('live_analysis_summary', summary_text, extension='txt', content_type='text/plain')
    cache_json_artifact('latest_live_analysis', result)
    return {
        'json_report': json_artifact,
        'summary_report': summary_artifact,
    }


def _validation_summary_from_rows(rows: list[dict[str, Any]], strategy: str) -> dict[str, Any]:
    if not rows:
        return {}
    summary = build_trade_evaluation_summary(rows, strategy_name=str(strategy or 'VINAYAK'))
    readiness = evaluate_readiness(rows, rows)
    return {
        'clean_trades': summary.get('clean_trades', summary.get('closed_trades', 0)),
        'expectancy_per_trade': summary.get('expectancy_per_trade', 0.0),
        'expectancy_stability_score': summary.get('expectancy_stability_score', 0.0),
        'profit_factor': summary.get('profit_factor', 0.0),
        'profit_factor_stability_score': summary.get('profit_factor_stability_score', 0.0),
        'max_drawdown_pct': summary.get('max_drawdown_pct', 0.0),
        'recovery_factor': summary.get('recovery_factor', 0.0),
        'pass_fail_status': summary.get('pass_fail_status', 'NEED_MORE_DATA'),
        'confidence_label': summary.get('confidence_label', 'NEED_MORE_DATA'),
        'paper_readiness_summary': summary.get('paper_readiness_summary', ''),
        'go_live_status': summary.get('go_live_status', 'PAPER_ONLY'),
        'promotion_status': summary.get('promotion_status', 'RESEARCH_ONLY'),
        'warnings': summary.get('warnings', []),
        'pass_fail_reasons': summary.get('pass_fail_reasons', []),
        'system_status': readiness.get('verdict', 'NOT_READY'),
        'readiness_reasons': readiness.get('reasons', []),
        'validation_pass_rate': readiness.get('validation_pass_rate', 0.0),
        'top_rejection_reasons': readiness.get('top_rejection_reasons', {}),
    }


def _data_status(candles: pd.DataFrame) -> dict[str, Any]:
    report = dict(getattr(candles, 'attrs', {}).get('cleaning_report', {}) or {})
    return {
        'status': 'VALID' if not candles.empty else 'INVALID',
        'rows': int(len(candles)),
        'latest_timestamp': report.get('latest_timestamp', str(candles.iloc[-1]['timestamp']) if not candles.empty else ''),
        'duplicates_removed': int(report.get('duplicates_removed', 0) or 0),
        'columns': list(report.get('columns', list(candles.columns))),
    }


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or '').strip()
    if not text:
        return None
    normalized = text.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = pd.to_datetime(text, errors='coerce', utc=True)
        if pd.isna(parsed):
            return None
        return parsed.to_pydatetime()
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _recent_market_snapshot(interval: str, *, max_age_seconds: int = MARKET_HEARTBEAT_MIN_REFRESH_SECONDS) -> dict[str, Any] | None:
    snapshot = get_observability_snapshot()
    latest_metric = dict(snapshot.get('metrics', {}).get('latest_data_timestamp', {}) or {})
    updated_at = _parse_iso_datetime(latest_metric.get('updated_at'))
    if updated_at is None:
        return None
    if (datetime.now(UTC) - updated_at).total_seconds() > max_age_seconds:
        return None
    market_data_delay = snapshot.get('metrics', {}).get('market_data_delay_seconds', {}).get('value', 0.0)
    rows_loaded = snapshot.get('metrics', {}).get('market_data_rows_loaded_total', {}).get('value', 0)
    return {
        'candles': [],
        'candle_count': 0,
        'data_status': {
            'status': 'VALID',
            'rows': int(rows_loaded or 0),
            'latest_timestamp': latest_metric.get('value', ''),
            'duplicates_removed': int(snapshot.get('metrics', {}).get('market_data_duplicates_total', {}).get('value', 0) or 0),
            'provider': '',
            'source': 'OBSERVABILITY_CACHE',
            'latest_interval': interval,
            'market_data_delay_seconds': float(market_data_delay or 0.0),
            'refresh_mode': 'CACHE_HIT',
        },
    }

def _update_observability_metrics_from_run(rows: list[dict[str, Any]], candles: pd.DataFrame) -> None:
    if candles is not None and not candles.empty:
        latest_timestamp = str(candles.iloc[-1]['timestamp'])
        set_metric('latest_data_timestamp', latest_timestamp)
        set_metric('market_data_rows_loaded_total', int(len(candles)))
        latest_dt = pd.to_datetime(candles.iloc[-1]['timestamp'], errors='coerce', utc=True)
        if not pd.isna(latest_dt):
            delay_seconds = max(0.0, (datetime.now(UTC) - latest_dt.to_pydatetime()).total_seconds())
            set_metric('market_data_delay_seconds', round(delay_seconds, 2))
        cleaning_report = dict(getattr(candles, 'attrs', {}).get('cleaning_report', {}) or {})
        set_metric('market_data_duplicates_total', int(cleaning_report.get('duplicates_removed', 0) or 0))
        set_metric('market_data_nulls_total', int(cleaning_report.get('null_rows_removed', 0) or 0))
        set_metric('schema_validation_failures_total', int(len(cleaning_report.get('issues', []) or [])))
    if not rows:
        set_metric('rolling_win_rate', 0.0)
        set_metric('rolling_expectancy', 0.0)
        set_metric('pnl_today', 0.0)
        return
    metrics = run_full_metrics_engine(rows, candles=candles)
    performance = dict(metrics.get('performance', {}) or {})
    execution = dict(metrics.get('execution', {}) or {})
    validation = dict(metrics.get('validation', {}) or {})
    set_metric('rolling_win_rate', round(float(performance.get('win_rate', 0.0)) * 100.0, 2))
    set_metric('rolling_expectancy', round(float(performance.get('expectancy', 0.0)), 4))
    set_metric('pnl_today', round(float(performance.get('net_profit', 0.0)), 2))
    set_metric('execution_success_rate', round(float(execution.get('execution_success_rate', 0.0)), 4))
    set_metric('validation_pass_rate', round(float(validation.get('validation_pass_rate', 0.0)), 4))
    set_metric('high_quality_setup_rate', round(float(validation.get('high_quality_setup_rate', 0.0)), 4))

def refresh_market_data_snapshot(
    *,
    symbol: str,
    interval: str,
    period: str,
    security_map_path: str = 'data/dhan_security_map.csv',
) -> dict[str, Any]:
    recent_snapshot = _recent_market_snapshot(interval)
    if recent_snapshot is not None:
        return {
            'symbol': symbol,
            'interval': interval,
            'period': period,
            **recent_snapshot,
        }

    live_rows = fetch_live_ohlcv(
        symbol=symbol,
        interval=interval,
        period=period,
        provider='DHAN',
        security_map_path=security_map_path,
        force_refresh=False,
    )
    live_rows = list(live_rows[-MARKET_HEARTBEAT_MAX_ROWS:])
    candles_df = prepare_trading_data(pd.DataFrame(live_rows))
    _update_observability_metrics_from_run([], candles_df)
    normalized_rows = _normalize_rows(live_rows)
    return {
        'symbol': symbol,
        'interval': interval,
        'period': period,
        'candles': normalized_rows,
        'candle_count': len(normalized_rows),
        'data_status': {
            **_data_status(candles_df),
            'provider': str((live_rows[-1] if live_rows else {}).get('provider', '') or ''),
            'source': str((live_rows[-1] if live_rows else {}).get('source', '') or ''),
            'latest_interval': str((live_rows[-1] if live_rows else {}).get('interval', interval) or interval),
            'refresh_mode': 'FETCHED',
        },
    }

def run_live_trading_analysis(
    *,
    symbol: str,
    interval: str,
    period: str,
    strategy: str,
    capital: float,
    risk_pct: float,
    rr_ratio: float,
    trailing_sl_pct: float,
    strike_step: int,
    moneyness: str,
    strike_steps: int,
    mtf_ema_period: int,
    mtf_setup_mode: str,
    mtf_retest_strength: bool,
    mtf_max_trades_per_day: int,
    entry_cutoff_hhmm: str = '',
    cost_bps: float = 0.0,
    fixed_cost_per_trade: float = 0.0,
    max_daily_loss: float | None = None,
    max_trades_per_day: int | None = None,
    max_position_value: float | None = None,
    max_open_positions: int | None = None,
    max_symbol_exposure_pct: float | None = None,
    max_portfolio_exposure_pct: float | None = None,
    max_open_risk_pct: float | None = None,
    kill_switch_enabled: bool = False,
    fetch_option_metrics: bool = False,
    send_telegram: bool = False,
    telegram_token: str = '',
    telegram_chat_id: str = '',
    auto_execute: bool = False,
    execution_type: str = 'NONE',
    lot_size: int = 0,
    lots: int = 0,
    security_map_path: str = 'data/dhan_security_map.csv',
    paper_log_path: str = str(DEFAULT_PAPER_LOG_PATH),
    live_log_path: str = str(DEFAULT_LIVE_LOG_PATH),
    db_session: Session | None = None,
) -> dict[str, Any]:
    import time
    trace_id = f"{symbol}_{strategy}_{int(time.time())}"
    started = time.perf_counter()
    fetch_started = time.perf_counter()
    live_rows = fetch_live_ohlcv(symbol=symbol, interval=interval, period=period, provider='DHAN', security_map_path=security_map_path, force_refresh=True)
    record_stage('market_fetch', status='SUCCESS', duration_seconds=round(time.perf_counter() - fetch_started, 4), symbol=symbol, strategy=strategy, message='Live market rows fetched', trace_id=trace_id)
    prep_started = time.perf_counter()
    candles_df = prepare_trading_data(pd.DataFrame(live_rows))
    candle_rows = df_to_candles(candles_df)
    record_stage('indicator_calc', status='SUCCESS', duration_seconds=round(time.perf_counter() - prep_started, 4), symbol=symbol, strategy=strategy, message='Data prepared for indicators', trace_id=trace_id)

    context = StrategyContext(
        strategy=strategy,
        candles=candles_df,
        candle_rows=candle_rows,
        capital=float(capital),
        risk_pct=float(risk_pct),
        rr_ratio=float(rr_ratio),
        trailing_sl_pct=float(trailing_sl_pct),
        symbol=str(symbol),
        strike_step=int(strike_step),
        moneyness=str(moneyness),
        strike_steps=int(strike_steps),
        fetch_option_metrics=False,
        mtf_ema_period=int(mtf_ema_period),
        mtf_setup_mode=str(mtf_setup_mode),
        mtf_retest_strength=bool(mtf_retest_strength),
        mtf_max_trades_per_day=int(mtf_max_trades_per_day),
        entry_cutoff=str(entry_cutoff_hhmm),
        cost_bps=float(cost_bps),
        fixed_cost_per_trade=float(fixed_cost_per_trade),
        max_daily_loss=max_daily_loss,
        max_trades_per_day=max_trades_per_day,
        max_position_value=max_position_value,
        max_open_positions=max_open_positions,
        max_symbol_exposure_pct=max_symbol_exposure_pct,
        max_portfolio_exposure_pct=max_portfolio_exposure_pct,
        max_open_risk_pct=max_open_risk_pct,
        kill_switch_enabled=kill_switch_enabled,
    )
    strategy_started = time.perf_counter()
    signal_rows = run_strategy_workflow(
        context,
        attach_levels_fn=attach_indicator_trade_levels,
        attach_option_strikes_fn=attach_option_strikes,
        attach_option_metrics_fn=lambda rows, **kwargs: rows,
    )
    record_stage('zone_detection', status='SUCCESS', duration_seconds=round(time.perf_counter() - strategy_started, 4), symbol=symbol, strategy=strategy, message='Strategy workflow completed', trace_id=trace_id)
    signal_rows = attach_option_metrics(signal_rows, symbol=symbol, fetch_option_metrics=fetch_option_metrics)
    signal_rows = attach_lots(signal_rows, lot_size=lot_size, lots=lots)
    signal_rows = _normalize_rows(signal_rows)
    side_counts = Counter(str(row.get('side', '') or '').upper() for row in signal_rows if row.get('side'))

    message_bus = build_message_bus()
    telegram_sent = False
    telegram_error = ''
    telegram_payload: dict[str, Any] | None = None
    if send_telegram and signal_rows:
        message = build_trade_summary(signal_rows)
        message_bus.publish(
            EVENT_NOTIFICATION_REQUESTED,
            {
                'channel': 'telegram',
                'telegram_token': telegram_token,
                'telegram_chat_id': telegram_chat_id,
                'message': message,
                'symbol': symbol,
                'strategy': strategy,
            },
            source='live_analysis',
        )
        try:
            telegram_payload = send_telegram_message(telegram_token, telegram_chat_id, message)
            telegram_sent = True
        except Exception as exc:
            telegram_error = str(exc)

    requested_execution_mode = str(execution_type or 'NONE').upper()
    execution_mode, execution_note = _resolve_workspace_auto_execution_mode(requested_execution_mode, auto_execute)
    execution_summary: dict[str, Any] = {
        'mode': execution_mode,
        'executed_count': 0,
        'blocked_count': 0,
        'error_count': 0,
        'skipped_count': 0,
        'duplicate_count': 0,
    }
    execution_rows: list[dict[str, Any]] = []
    if auto_execute and execution_mode in {'PAPER', 'LIVE'} and signal_rows:
        _candidates, result = execute_workspace_candidates(
            strategy,
            symbol,
            candles_df,
            signal_rows,
            execution_mode=execution_mode,
            paper_log_path=str(paper_log_path),
            live_log_path=str(live_log_path),
            capital=capital,
            per_trade_risk_pct=risk_pct,
            max_trades_per_day=max_trades_per_day,
            max_daily_loss=max_daily_loss,
            max_position_value=max_position_value,
            max_open_positions=max_open_positions,
            max_symbol_exposure_pct=max_symbol_exposure_pct,
            max_portfolio_exposure_pct=max_portfolio_exposure_pct,
            max_open_risk_pct=max_open_risk_pct,
            kill_switch_enabled=kill_switch_enabled,
            security_map_path=str(security_map_path),
            resolve_live_kwargs=_resolve_live_execution_kwargs,
            db_session=db_session,
        )
        execution_summary = {
            'mode': execution_mode,
            'executed_count': result.executed_count,
            'blocked_count': result.blocked_count,
            'error_count': result.error_count,
            'skipped_count': result.skipped_count,
            'duplicate_count': result.duplicate_count,
        }
        execution_rows = _normalize_rows(result.rows)

    record_stage('trade_build', status='SUCCESS', symbol=symbol, strategy=strategy, message='Trade rows built', trace_id=trace_id)
    validation_rows = execution_rows if execution_rows else signal_rows
    validation_summary = _validation_summary_from_rows(validation_rows, strategy)
    _update_observability_metrics_from_run(validation_rows, candles_df)
    alert_notifications_sent = publish_active_alerts(
        message_bus=message_bus,
        telegram_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
        source='live_analysis_alerting',
    )

    total_duration = round(time.perf_counter() - started, 4)
    set_metric('trading_cycle_duration_seconds', total_duration)
    log_event(component='trading_workspace', event_name='live_analysis_run', symbol=symbol, strategy=strategy, severity='INFO', message='Live analysis run completed', context_json={'signal_count': len(signal_rows), 'execution_rows': len(execution_rows), 'duration_seconds': total_duration, 'trace_id': trace_id})
    response: dict[str, Any] = {
        'symbol': symbol,
        'interval': interval,
        'period': period,
        'strategy': strategy,
        'candle_count': len(live_rows),
        'signal_count': len(signal_rows),
        'side_counts': dict(side_counts),
        'candles': _normalize_rows(live_rows),
        'signals': signal_rows,
        'generated_at': datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'telegram_sent': telegram_sent,
        'telegram_error': telegram_error,
        'telegram_payload': telegram_payload or {},
        'execution_note': execution_note,
        'execution_summary': execution_summary,
        'execution_rows': execution_rows,
        'validation_summary': validation_summary,
        'data_status': {**_data_status(candles_df), 'provider': str((live_rows[-1] if live_rows else {}).get('provider', '') or ''), 'source': str((live_rows[-1] if live_rows else {}).get('source', '') or ''), 'latest_interval': str((live_rows[-1] if live_rows else {}).get('interval', interval) or interval)},
        'system_status': validation_summary.get('system_status', 'NOT_READY'),
        'alert_notifications_sent': alert_notifications_sent,
    }
    response['report_artifacts'] = _build_report_artifacts(response)
    message_bus.publish(
        EVENT_ANALYSIS_COMPLETED,
        {
            'symbol': symbol,
            'strategy': strategy,
            'interval': interval,
            'period': period,
            'signal_count': len(signal_rows),
            'execution_mode': execution_mode,
            'requested_execution_mode': requested_execution_mode,
            'report_artifacts': response['report_artifacts'],
        },
        source='live_analysis',
    )
    return response





























