from __future__ import annotations

import sys
import types
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import src.trading_runtime_service as trading_runtime_service
from src.amd_fvg_sd_bot import generate_trades as generate_amd_fvg_sd_trades
from src.breakout_bot import generate_trades as generate_breakout_trades
from src.strategy_demand_supply import generate_trades as generate_demand_supply_trades
from src.indicator_bot import generate_indicator_rows
from src.runtime_defaults import (
    APP_LOG,
    BROKER_OPTIONS,
    DEFAULT_INTERVAL,
    DEFAULT_SYMBOL,
    ERRORS_LOG,
    EXECUTED_TRADES_OUTPUT,
    EXECUTION_LOG,
    MODE_OPTIONS,
    REJECTIONS_LOG,
    STRATEGY_OPTIONS,
    TIMEFRAME_OPTIONS,
    runtime_log_paths,
    runtime_output_paths,
)
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.runtime_strategy_presets import OPERATOR_DEFAULTS
from src.strike_selector import attach_option_strikes
from src.trading_core import append_log, configure_file_logging
from src.runtime_models import period_for_interval
from src.output_decision_service import (
    build_blocker_frame,
    build_plain_english_next_action,
    build_quality_ladder_frame,
    build_quality_ladder_summary,
    build_top_fix_actions,
)
from src.backtesting.score_backtest_metrics import render_score_backtest_summary
from src.strategies.supply_demand import detect_scored_zones, zone_records_to_rows
from src.visualization.zone_heatmap import build_zone_heatmap
from src.trading_runtime_service import latest_actionable_trades, run_operator_action
from src.trading_ui_service import apply_minimal_theme, build_request, initialize_ui_runtime, log_ui_event, render_operator_panels, render_summary_cards


configure_file_logging()

_attach_option_metrics = trading_runtime_service._attach_option_metrics


def fetch_ohlcv_data(symbol: str, interval: str = DEFAULT_INTERVAL, period: str = trading_runtime_service.DEFAULT_PERIOD) -> pd.DataFrame:
    return trading_runtime_service.fetch_ohlcv_data(symbol, interval=interval, period=period)


def run_strategy(**kwargs):
    trading_runtime_service.generate_breakout_trades = generate_breakout_trades
    trading_runtime_service.generate_demand_supply_trades = generate_demand_supply_trades
    trading_runtime_service.generate_amd_fvg_sd_trades = generate_amd_fvg_sd_trades
    trading_runtime_service.generate_indicator_rows = generate_indicator_rows
    trading_runtime_service.generate_mtf_trade_trades = generate_mtf_trade_trades
    trading_runtime_service.attach_option_strikes = attach_option_strikes
    trading_runtime_service._attach_option_metrics = _attach_option_metrics
    return trading_runtime_service.run_strategy(**kwargs)


def _ensure_output_files() -> None:
    initialize_ui_runtime(runtime_output_paths(), runtime_log_paths())


def _minimal_theme() -> None:
    apply_minimal_theme()


def _render_summary_cards(
    trades: list[dict[str, object]],
    summary: dict[str, object],
    todays_trades: int,
    candles: pd.DataFrame | None = None,
    market_data_summary: dict[str, object] | None = None,
) -> None:
    render_summary_cards(trades, summary, todays_trades, candles, market_data_summary)


def _render_operator_panels(status: str, trades: list[dict[str, object]], symbol: str, timeframe: str, period: str, broker_choice: str, broker_status: str) -> None:
    render_operator_panels(status, trades, symbol, timeframe, period, broker_choice, broker_status)


def _build_request(strategy: str, symbol: str, timeframe: str, capital: float, risk_pct: float, rr_ratio: float, mode: str, broker_choice: str, run_clicked: bool, backtest_clicked: bool):
    return build_request(strategy, symbol, timeframe, capital, risk_pct, rr_ratio, mode, broker_choice, run_clicked, backtest_clicked)


def _append_text_log(path: Path, message: str) -> None:
    log_ui_event(path, message)


def _safe_float(value: object) -> float:
    try:
        if value is None or str(value).strip() == '':
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: object) -> int:
    try:
        if value is None or str(value).strip() == '':
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _format_status_label(value: str) -> str:
    normalized = str(value or '').strip().upper()
    mapping = {
        'FAIL_NOT_READY': 'Fail: Not Ready',
        'PASS_FOR_SMALL_CAPITAL': 'Pass: Small Capital',
        'PAPER_ONLY': 'Paper Only',
        'LIVE_LOCKED': 'Live Locked',
        'LIVE_ELIGIBLE': 'Live Eligible',
        'PAPER_ACTIVE | LIVE_LOCKED': 'Paper Active | Live Locked',
        'PAPER_ACTIVE | LIVE LOCKED': 'Paper Active | Live Locked',
    }
    if normalized in mapping:
        return mapping[normalized]
    if not normalized:
        return 'Pending'
    return str(value).replace('_', ' ').title()


def _metric_display(value: object, *, kind: str = 'number') -> str:
    raw = str(value or '').strip()
    if kind == 'trades':
        numeric = _safe_int(value)
        return 'No sample yet' if numeric <= 0 else f'{numeric} Trades'
    if kind == 'proof':
        return 'Yes' if raw.upper() == 'YES' else 'No'
    if kind == 'factor':
        if raw == '' or _safe_float(value) <= 0:
            return 'Pending backtest'
        return raw
    numeric = _safe_float(value)
    if numeric <= 0:
        return 'Pending backtest'
    return f'{numeric:.2f}' if kind == 'pct' else f'{numeric:.2f}'


def _score_color(score: float) -> str:
    value = float(score)
    if value >= 8.0:
        return '#16a34a'
    if value >= 6.0:
        return '#d97706'
    return '#dc2626'


def _scorecard_styler(rows: list[dict[str, object]]):
    frame = pd.DataFrame(rows)
    return frame.style.map(lambda value: f'color: {_score_color(float(value))}; font-weight: 700;', subset=['score'])


def _build_scorecard_rows(summary: dict[str, object], *, status: str, todays_trades: int) -> list[dict[str, object]]:
    total_trades = _safe_int(summary.get('total_trades', summary.get('closed_trades', 0)))
    avg_trades_per_day = _safe_float(summary.get('avg_trades_per_day'))
    duplicate_rejections = _safe_int(summary.get('duplicate_rejections'))
    risk_rule_rejections = _safe_int(summary.get('risk_rule_rejections'))
    profit_factor = _safe_float(summary.get('profit_factor'))
    expectancy = _safe_float(summary.get('expectancy_per_trade'))
    max_drawdown_pct = _safe_float(summary.get('max_drawdown_pct'))
    deployment_ready = str(summary.get('deployment_ready', '') or '').strip().upper() == 'YES'
    sample_window_passed = str(summary.get('sample_window_passed', '') or '').strip().upper() == 'YES'
    validation_available = bool(summary) and total_trades > 0

    trade_quality_score = 5.0
    if avg_trades_per_day > 0 and avg_trades_per_day <= 1.5:
        trade_quality_score += 1.5
    if todays_trades <= 1:
        trade_quality_score += 1.0
    if duplicate_rejections == 0:
        trade_quality_score += 0.5
    if deployment_ready:
        trade_quality_score += 1.0
    trade_quality_issue = 'Fresh validation missing for trade selectivity.'
    trade_quality_fix = 'Run a 150-200 trade backtest and keep avg trades/day near 1.'
    if validation_available and avg_trades_per_day > 1.5:
        trade_quality_issue = f'Overtrading persists at {avg_trades_per_day:.2f} trades/day.'
        trade_quality_fix = 'Raise the score floor or retest quality filters until trade density falls.'
    elif validation_available and deployment_ready:
        trade_quality_issue = 'Retest, VWAP, and session filters are behaving within the target window.'
        trade_quality_fix = 'Keep Balanced mode as the default and review only if trade density rises again.'

    validation_score = 4.0
    if validation_available:
        validation_score += 1.5 if sample_window_passed else 0.0
        validation_score += 1.5 if expectancy > 0 else 0.0
        validation_score += 1.5 if profit_factor >= 1.3 else 0.0
        validation_score += 1.5 if 0 < max_drawdown_pct <= 10.0 else 1.0 if max_drawdown_pct == 0 else 0.0
    validation_issue = 'No fresh backtest validation loaded in the UI.'
    validation_fix = 'Run Backtest to populate expectancy, profit factor, drawdown, and pass/fail gates.'
    if validation_available and not sample_window_passed:
        validation_issue = f'Sample window failed with {total_trades} trades.'
        validation_fix = 'Keep validation only in the 150-200 trade window before considering deployment.'
    elif validation_available and not deployment_ready:
        validation_issue = build_plain_english_next_action(summary)
        validation_fix = 'Top fix: ' + build_top_fix_actions(summary, limit=1)[0]
    elif validation_available and deployment_ready:
        validation_issue = 'Expectancy, profit factor, drawdown, and sample size passed current gates.'
        validation_fix = 'Continue validating on rolling samples before any live change.'

    execution_score = 5.0
    if duplicate_rejections == 0:
        execution_score += 2.0
    if risk_rule_rejections == 0:
        execution_score += 1.5
    if not _result_failed(status):
        execution_score += 1.5
    execution_issue = 'Execution discipline needs a fresh validated run.'
    execution_fix = 'Use Run or Backtest and confirm duplicate/risk rejections stay at zero.'
    if validation_available and duplicate_rejections > 0:
        execution_issue = f'Duplicate rejections detected: {duplicate_rejections}.'
        execution_fix = 'Tighten one-signal-one-trade rules until duplicates remain at zero.'
    elif validation_available and risk_rule_rejections > 0:
        execution_issue = f'Risk-rule rejections detected: {risk_rule_rejections}.'
        execution_fix = 'Reduce signal density or daily trade limits so valid candidates are not being discarded.'
    elif validation_available and duplicate_rejections == 0 and risk_rule_rejections == 0:
        execution_issue = 'Cooldowns, duplicate prevention, and daily limits are clean on the current sample.'
        execution_fix = 'Keep rejection logs monitored and block live trading unless deployment_ready=YES.'

    return [
        {'area': 'Trade Quality', 'score': round(min(trade_quality_score, 10.0), 1), 'current issue': trade_quality_issue, 'exact next fix': trade_quality_fix},
        {'area': 'Validation Metrics', 'score': round(min(validation_score, 10.0), 1), 'current issue': validation_issue, 'exact next fix': validation_fix},
        {'area': 'Execution Discipline', 'score': round(min(execution_score, 10.0), 1), 'current issue': execution_issue, 'exact next fix': execution_fix},
    ]


def _scorecard_detail_map(summary: dict[str, object], *, status: str, todays_trades: int, strategy_label: str) -> dict[str, list[str]]:
    blockers = str(summary.get('deployment_blockers', '') or '').strip() or 'None'
    top_fixes = build_top_fix_actions(summary)
    blocker_frame = build_blocker_frame(summary)
    grouped = ', '.join(f"{row['severity']}: {row['headline']}" for row in blocker_frame.to_dict(orient='records')[:3])
    return {
        'Trade Quality': [
            f"Operator strategy: {strategy_label}",
            f"Today's trades: {todays_trades}",
            f"Total validated trades: {_safe_int(summary.get('total_trades', summary.get('closed_trades', 0)))}",
            f"Avg trades/day: {_safe_float(summary.get('avg_trades_per_day')):.2f}",
            f"Sample window passed: {str(summary.get('sample_window_passed', 'NO') or 'NO')}",
            f"Deployment ready: {str(summary.get('deployment_ready', 'NO') or 'NO')}",
        ],
        'Validation Metrics': [
            f"Profit factor: {summary.get('profit_factor', 0.0)}",
            f"Expectancy/trade: {_safe_float(summary.get('expectancy_per_trade')):.2f}",
            f"Max drawdown %: {_safe_float(summary.get('max_drawdown_pct')):.2f}",
            f"Win rate: {_safe_float(summary.get('win_rate')):.2f}",
            f"Blocker groups: {grouped}",
            f"Top fix 1: {top_fixes[0]}",
            f"Raw blockers: {blockers}",
        ],
        'Execution Discipline': [
            f"Duplicate rejections: {_safe_int(summary.get('duplicate_rejections'))}",
            f"Risk-rule rejections: {_safe_int(summary.get('risk_rule_rejections'))}",
            f"Status: {status}",
            (
                'Execution rules are working, but strategy validation is still failing.'
                if str(summary.get('validation_passed', summary.get('deployment_ready', 'NO')) or 'NO').upper() != 'YES'
                else 'Execution rules are working and strategy validation is currently passing.'
            ),
        ],
    }


def _render_scorecard(summary: dict[str, object], status: str, todays_trades: int, strategy_label: str) -> None:
    rows = _build_scorecard_rows(summary, status=status, todays_trades=todays_trades)
    details = _scorecard_detail_map(summary, status=status, todays_trades=todays_trades, strategy_label=strategy_label)
    st.markdown('### Current-State Scorecard')
    st.caption('Green = strong, amber = watchlist, red = needs action.')
    st.dataframe(_scorecard_styler(rows), width='stretch', hide_index=True)
    for row in rows:
        with st.expander(f"Why: {row['area']}"):
            for line in details.get(str(row['area']), []):
                st.markdown(f'- {line}')


def _latest_actionable_trades(trades: list[dict[str, object]]) -> list[dict[str, object]]:
    return latest_actionable_trades(trades)


def _result_failed(status: str) -> bool:
    normalized = str(status or '').strip().lower()
    return normalized.startswith('run failed:') or normalized.startswith('backtest failed:')


def _render_execution_feedback(messages: list[tuple[str, str]]) -> None:
    for level, message in messages:
        normalized_level = str(level or '').strip().lower()
        text = str(message or '').strip()
        if not text:
            continue
        if normalized_level == 'error':
            st.error(text)
        elif normalized_level == 'warning':
            st.warning(text)
        elif normalized_level == 'success':
            st.success(text)
        else:
            st.info(text)


def _build_validation_snapshot(summary: dict[str, object]) -> pd.DataFrame:
    if not summary:
        return pd.DataFrame([{'metric': 'Validation state', 'value': 'No backtest loaded', 'target': 'Run Backtest', 'status': 'WATCH'}])
    total_trades = _safe_int(summary.get('total_trades', summary.get('closed_trades', 0)))
    expectancy = _safe_float(summary.get('expectancy_per_trade'))
    profit_factor = _safe_float(summary.get('profit_factor'))
    drawdown = _safe_float(summary.get('max_drawdown_pct'))
    duplicates = _safe_int(summary.get('duplicate_rejections'))
    deployment_ready = str(summary.get('deployment_ready', 'NO') or 'NO').upper()
    sample_window = str(summary.get('sample_window_passed', 'NO') or 'NO').upper()
    return _arrow_safe_frame(pd.DataFrame([
        {'metric': 'Sample size', 'value': total_trades, 'target': '150-200 trades', 'status': 'PASS' if sample_window == 'YES' else 'WATCH'},
        {'metric': 'Expectancy', 'value': round(expectancy, 2), 'target': '> 0', 'status': 'PASS' if expectancy > 0 else 'FAIL'},
        {'metric': 'Profit factor', 'value': round(profit_factor, 2), 'target': '> 1.30', 'status': 'PASS' if profit_factor > 1.3 else 'FAIL'},
        {'metric': 'Max drawdown %', 'value': round(drawdown, 2), 'target': '<= 10', 'status': 'PASS' if 0 <= drawdown <= 10 else 'FAIL'},
        {'metric': 'Duplicate rejections', 'value': duplicates, 'target': '0', 'status': 'PASS' if duplicates == 0 else 'FAIL'},
        {'metric': 'Deployment ready', 'value': deployment_ready, 'target': 'YES after all gates pass', 'status': 'PASS' if deployment_ready == 'YES' else 'WATCH'},
    ]))


def _build_signal_table(trades: list[dict[str, object]]) -> pd.DataFrame:
    actionable = _latest_actionable_trades(trades)
    if not actionable:
        return pd.DataFrame([{'timestamp': 'n/a', 'side': 'NONE', 'entry': 0.0, 'stop_loss': 0.0, 'target': 0.0, 'score': 0.0, 'reason': 'No actionable trade generated.'}])
    frame = pd.DataFrame(actionable)
    preferred_columns = ['timestamp', 'side', 'entry', 'stop_loss', 'target', 'score', 'reason']
    available_columns = [column for column in preferred_columns if column in frame.columns]
    return frame[available_columns].tail(5) if available_columns else frame.tail(5)


def _safe_tail_dataframe(path: Path, columns: list[str], rows: int = 8) -> pd.DataFrame:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return pd.DataFrame(columns=columns)
        frame = pd.read_csv(path)
        if frame.empty:
            return pd.DataFrame(columns=columns)
        available_columns = [column for column in columns if column in frame.columns]
        if available_columns:
            frame = frame[available_columns]
        return frame.tail(rows)
    except Exception:
        return pd.DataFrame(columns=columns)


def _display_value(value: object) -> str:
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'YES' if value else 'NO'
    if isinstance(value, float):
        if pd.isna(value):
            return ''
        return f'{value:.2f}' if value != int(value) else str(int(value))
    return str(value)


def _arrow_safe_frame(frame: pd.DataFrame) -> pd.DataFrame:
    safe = frame.copy()
    for column in safe.columns:
        if str(safe[column].dtype) == 'object':
            safe[column] = safe[column].map(_display_value)
    return safe


def _safe_log_preview(path: Path, lines: int = 10) -> str:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return 'No log entries available.'
        content = path.read_text(encoding='utf-8', errors='ignore').splitlines()
        return '\n'.join(content[-lines:]) if content else 'No log entries available.'
    except Exception as exc:
        return f'Unable to read log preview: {exc}'


def _header_badge(summary: dict[str, object], broker_choice: str) -> str:
    deployment_ready = str(summary.get('deployment_ready', 'NO') or 'NO').upper() == 'YES'
    if broker_choice == 'Paper' or not deployment_ready:
        return 'Paper Active | Live Locked'
    return 'Live Eligible'


def _header_go_live(summary: dict[str, object]) -> str:
    deployment_ready = str(summary.get('deployment_ready', 'NO') or 'NO').upper() == 'YES'
    validation_passed = str(summary.get('validation_passed', summary.get('deployment_ready', 'NO')) or 'NO').upper() == 'YES'
    if deployment_ready and validation_passed:
        return 'Pass: Small Capital'
    if validation_passed:
        return 'Paper Only'
    return 'Fail: Not Ready'


def _header_next_action(summary: dict[str, object], broker_choice: str) -> str:
    deployment_ready = str(summary.get('deployment_ready', 'NO') or 'NO').upper() == 'YES'
    total_trades = _safe_int(summary.get('total_trades', summary.get('closed_trades', 0)))
    expectancy = _safe_float(summary.get('expectancy_per_trade'))
    profit_factor = _safe_float(summary.get('profit_factor'))
    drawdown_proven = str(summary.get('drawdown_proven', 'NO') or 'NO').upper() == 'YES'
    if deployment_ready and broker_choice != 'Paper':
        return 'Live can remain locked until operator approval is explicit.'
    if deployment_ready:
        return 'Run paper execution and verify clean logs before any live promotion.'
    if total_trades < 150 and expectancy <= 0:
        return 'This strategy is not ready yet because not enough trades yet, expectancy is not positive. Run a clean backtest until the system has 150 to 200 validated trades.'
    if total_trades < 150:
        return 'Run a clean backtest until the system has 150 to 200 validated trades.'
    if str(summary.get('deployment_blockers', '') or '').strip():
        return build_plain_english_next_action(summary)
    if expectancy <= 0 or profit_factor <= 1.3 or not drawdown_proven:
        return 'This strategy is not ready yet because expectancy is not positive. Run a clean backtest until the system has 150 to 200 validated trades.' if expectancy <= 0 else 'Improve trade quality first. The system still needs profit factor above 1.3 and proven drawdown behavior.'
    return build_quality_ladder_summary(summary)


def _render_header(strategy: str, symbol: str, timeframe: str, mode: str, broker_choice: str, summary: dict[str, object]) -> None:
    go_live_status = _header_go_live(summary)
    live_permission = 'Live Locked' if broker_choice == 'Paper' or go_live_status != 'Pass: Small Capital' else 'Live Eligible'
    total_trades = _safe_int(summary.get('total_trades', summary.get('closed_trades', 0)))
    expectancy = _metric_display(summary.get('expectancy_per_trade'))
    second_half_expectancy = _metric_display(summary.get('second_half_expectancy_per_trade'))
    profit_factor = _metric_display(summary.get('profit_factor'), kind='factor')
    max_drawdown_pct = _metric_display(summary.get('max_drawdown_pct'), kind='pct')
    drawdown_proven = _metric_display(summary.get('drawdown_proven', 'NO'), kind='proof')
    next_action = _header_next_action(summary, broker_choice)
    st.markdown(
        (
            '<div style="background:linear-gradient(145deg,#0f172a 0%,#111827 55%,#1f2937 100%);border:1px solid rgba(148,163,184,0.18);border-radius:24px;padding:24px 24px 18px 24px;box-shadow:0 18px 45px rgba(15,23,42,0.18);margin-bottom:16px;">'
            '<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;">'
            '<div>'
            '<div style="font-size:13px;letter-spacing:0.12em;text-transform:uppercase;color:#fca5a5;font-weight:700;">Validation-First Operator Surface</div>'
            '<h2 style="margin:8px 0 0 0;color:#f8fafc;font-size:40px;line-height:1.05;">Production Trading Desk</h2>'
            '<p style="margin:10px 0 0 0;color:#cbd5e1;font-size:15px;max-width:760px;">Retest-only Nifty intraday deployment console with VWAP discipline, strict session filtering, real drawdown proof, and hard go-live gates.</p>'
            '</div>'
            f'<div style="padding:12px 16px;border-radius:999px;background:#1e293b;border:1px solid rgba(248,250,252,0.12);color:#f8fafc;font-size:13px;font-weight:800;letter-spacing:0.08em;">{_header_badge(summary, broker_choice)}</div>'
            '</div>'
            '<div style="display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:12px;margin-top:18px;">'
            f'<div style="background:rgba(15,23,42,0.55);border:1px solid rgba(148,163,184,0.18);border-radius:18px;padding:14px;"><div style="font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.08em;">Go-Live Status</div><div style="margin-top:6px;font-size:20px;font-weight:800;color:#f8fafc;">{go_live_status}</div></div>'
            f'<div style="background:rgba(15,23,42,0.55);border:1px solid rgba(148,163,184,0.18);border-radius:18px;padding:14px;"><div style="font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.08em;">Live Permission</div><div style="margin-top:6px;font-size:20px;font-weight:800;color:#f8fafc;">{live_permission}</div></div>'
            f'<div style="background:rgba(15,23,42,0.55);border:1px solid rgba(148,163,184,0.18);border-radius:18px;padding:14px;"><div style="font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.08em;">Active Strategy</div><div style="margin-top:6px;font-size:20px;font-weight:800;color:#f8fafc;">{strategy}</div><div style="margin-top:6px;font-size:13px;color:#cbd5e1;">{symbol} | {timeframe} | {mode}</div></div>'
            f'<div style="background:rgba(15,23,42,0.55);border:1px solid rgba(148,163,184,0.18);border-radius:18px;padding:14px;"><div style="font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.08em;">Validation Window</div><div style="margin-top:6px;font-size:20px;font-weight:800;color:#f8fafc;">{_metric_display(total_trades, kind='trades')}</div><div style="margin-top:6px;font-size:13px;color:#cbd5e1;">Target: 150-200 clean trades</div></div>'
            '</div>'
            '<div style="display:grid;grid-template-columns:repeat(4,minmax(140px,1fr));gap:12px;margin-top:12px;">'
            f'<div style="background:rgba(30,41,59,0.82);border-radius:16px;padding:12px;"><div style="font-size:12px;color:#94a3b8;">Expectancy/Trade</div><div style="margin-top:4px;font-size:18px;font-weight:800;color:#f8fafc;">{expectancy}</div></div>'
            f'<div style="background:rgba(30,41,59,0.82);border-radius:16px;padding:12px;"><div style="font-size:12px;color:#94a3b8;">2H Expectancy</div><div style="margin-top:4px;font-size:18px;font-weight:800;color:#f8fafc;">{second_half_expectancy}</div></div>'
            f'<div style="background:rgba(30,41,59,0.82);border-radius:16px;padding:12px;"><div style="font-size:12px;color:#94a3b8;">Profit Factor</div><div style="margin-top:4px;font-size:18px;font-weight:800;color:#f8fafc;">{profit_factor}</div></div>'
            f'<div style="background:rgba(30,41,59,0.82);border-radius:16px;padding:12px;"><div style="font-size:12px;color:#94a3b8;">Drawdown</div><div style="margin-top:4px;font-size:18px;font-weight:800;color:#f8fafc;">{max_drawdown_pct if max_drawdown_pct == 'Pending backtest' else max_drawdown_pct + '%'}</div><div style="margin-top:4px;font-size:12px;color:#cbd5e1;">Proof: {drawdown_proven}</div></div>'
            '</div>'
            f'<div style="margin-top:14px;padding:14px 16px;border-radius:16px;background:rgba(248,250,252,0.06);border:1px solid rgba(248,250,252,0.08);"><div style="font-size:12px;color:#94a3b8;text-transform:uppercase;letter-spacing:0.08em;">Next Action</div><div style="margin-top:6px;color:#f8fafc;font-size:15px;font-weight:600;">{next_action}</div></div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )


def _render_quality_ladder(summary: dict[str, object]) -> None:
    st.markdown('### Strategy Quality Ladder')
    st.caption('These four checks explain why the system is improving or failing in plain English.')
    st.info(build_quality_ladder_summary(summary))
    frame = build_quality_ladder_frame(summary)
    st.dataframe(_arrow_safe_frame(frame), width='stretch', hide_index=True)


def _button_clicked(button_host: object, label: str, *, legacy_label: str = '', **kwargs: object) -> bool:
    clicked = bool(button_host.button(label, **kwargs))
    if clicked:
        return True
    if legacy_label and not isinstance(st, types.ModuleType):
        return bool(button_host.button(legacy_label, **kwargs))
    return False


def _render_dashboard_tab(*, strategy: str, symbol: str, timeframe: str, period: str, broker_choice: str, status: str, broker_status: str, trades: list[dict[str, object]], active_summary: dict[str, object], scorecard_summary: dict[str, object], todays_trades: int, candles: pd.DataFrame | None = None, market_data_summary: dict[str, object] | None = None) -> None:
    _render_summary_cards(trades, active_summary, todays_trades, candles, market_data_summary)
    left, right = st.columns(2)
    with left:
        st.markdown('### Recent Signals')
        st.caption(f'Latest actionable setups for {symbol} on {timeframe} candles.')
        st.dataframe(_arrow_safe_frame(_build_signal_table(trades)), width='stretch', hide_index=True)
    with right:
        _render_operator_panels(status, trades, symbol, timeframe, period, broker_choice, broker_status)
    _render_quality_ladder(scorecard_summary)
    _render_scorecard(scorecard_summary, status, todays_trades, strategy)


def _render_charts_tab(*, candles: pd.DataFrame, symbol: str, timeframe: str) -> None:
    st.markdown('### Market Overview')
    if candles.empty:
        st.info('No market sample loaded yet. Run Backtest or Start Paper to load price context.')
        return
    latest_close = float(candles['close'].iloc[-1])
    latest_high = float(candles['high'].iloc[-1])
    latest_low = float(candles['low'].iloc[-1])
    latest_volume = float(candles['volume'].iloc[-1]) if 'volume' in candles.columns else 0.0
    overview = st.columns(4)
    overview[0].metric('Close', round(latest_close, 2))
    overview[1].metric('High', round(latest_high, 2))
    overview[2].metric('Low', round(latest_low, 2))
    overview[3].metric('Volume', int(latest_volume))
    st.markdown('### Price Chart')
    chart = (
        alt.Chart(candles.tail(240))
        .mark_line(color='#7a8b5a', strokeWidth=2)
        .encode(
            x=alt.X('timestamp:T', title='Time'),
            y=alt.Y('close:Q', title='Close Price'),
            tooltip=['timestamp:T', 'open:Q', 'high:Q', 'low:Q', 'close:Q', 'volume:Q'],
        )
        .properties(height=360)
        .interactive()
    )
    st.altair_chart(chart, width='stretch')
    with st.expander('Recent Candle Data'):
        st.dataframe(_arrow_safe_frame(candles.tail(20)), width='stretch', hide_index=True)
    st.caption(f'{symbol} | {timeframe} | showing latest {min(len(candles), 240)} candles')


def _render_score_backtest_report_tab(*, trades: list[dict[str, object]], summary: dict[str, object]) -> None:
    st.markdown('### Score Backtest Report')
    if not trades:
        st.info('No backtest trades available yet. Run Backtest to evaluate score buckets and thresholds.')
        return
    analysis = render_score_backtest_summary(trades, starting_equity=max(_safe_float(summary.get('starting_equity', 100000.0)), 1.0))
    bucket_rows = pd.DataFrame(analysis.get('score_bucket_rows', []))
    threshold_rows = pd.DataFrame(analysis.get('threshold_filter_rows', []))
    col_one, col_two, col_three = st.columns(3)
    col_one.metric('Best Min Score', str(analysis.get('best_min_score_threshold', 'ALL')))
    col_two.metric('Higher Score Improves Win Rate', str(summary.get('higher_score_improves_win_rate', 'INSUFFICIENT_DATA') or 'INSUFFICIENT_DATA'))
    col_three.metric('Higher Score Improves Expectancy', str(summary.get('higher_score_improves_expectancy', 'INSUFFICIENT_DATA') or 'INSUFFICIENT_DATA'))
    st.markdown('#### Win Rate by Score Bucket')
    if not bucket_rows.empty:
        display = bucket_rows.rename(columns={
            'bucket': 'Score bucket',
            'trades': 'Trades',
            'wins': 'Wins',
            'losses': 'Losses',
            'win_rate': 'Win rate',
            'avg_pnl': 'Average PnL',
            'expectancy': 'Expectancy',
            'profit_factor': 'Profit factor',
            'max_drawdown_pct': 'Max DD %',
        })
        st.dataframe(_arrow_safe_frame(display), width='stretch', hide_index=True)
    st.markdown('#### Threshold Comparison')
    if not threshold_rows.empty:
        display = threshold_rows.rename(columns={
            'threshold_label': 'Filter',
            'trades': 'Trades',
            'win_rate': 'Win rate',
            'expectancy': 'Expectancy',
            'total_pnl': 'Total PnL',
            'max_drawdown_pct': 'Max DD %',
            'profit_factor': 'Profit factor',
        })
        st.dataframe(_arrow_safe_frame(display), width='stretch', hide_index=True)


def _render_zone_heatmap_tab(*, candles: pd.DataFrame, symbol: str) -> None:
    st.markdown('### Zone Heatmap')
    if candles.empty:
        st.info('No candle sample available yet. Run Backtest or Start Paper to render the zone heatmap.')
        return
    candle_rows = candles.to_dict(orient='records')
    zone_records = detect_scored_zones(candle_rows, symbol=symbol)
    if not zone_records:
        st.info('No supply or demand zones were detected in the current candle sample.')
        return
    zone_rows = zone_records_to_rows(zone_records)
    heatmap = build_zone_heatmap(candles, zone_rows)
    st.altair_chart(heatmap, width='stretch')
    with st.expander('Zone Table'):
        st.dataframe(_arrow_safe_frame(pd.DataFrame(zone_rows)), width='stretch', hide_index=True)


def _render_trades_tab(*, trades: list[dict[str, object]], status: str) -> None:
    st.markdown('### Trade Explorer')
    if not trades:
        st.info('No trade rows available yet. Run Backtest for research output or Start Paper for paper execution.')
        return
    trades_frame = pd.DataFrame(trades)
    preferred_columns = [
        'timestamp', 'signal_time', 'strategy', 'symbol', 'side', 'entry', 'entry_price', 'stop_loss', 'target',
        'score', 'reason', 'trade_status', 'validation_status', 'duplicate_reason'
    ]
    available_columns = [column for column in preferred_columns if column in trades_frame.columns]
    st.dataframe(_arrow_safe_frame(trades_frame[available_columns] if available_columns else trades_frame), width='stretch', hide_index=True)
    st.caption(f'Trade rows: {len(trades_frame)} | Status: {status}')


def _render_validation_tab(summary: dict[str, object]) -> None:
    st.markdown('### Validation Summary')
    blockers = str(summary.get('deployment_blockers', '') or '').strip()
    blocker_count = 0 if not blockers else len([part for part in blockers.split(';') if str(part).strip()])
    plain_summary = build_quality_ladder_summary(summary)
    col_one, col_two, col_three, col_four = st.columns(4)
    col_one.metric('Deployment Ready', str(summary.get('deployment_ready', 'NO') or 'NO'))
    col_two.metric('Sample Window', str(summary.get('sample_window_passed', 'NO') or 'NO'))
    col_three.metric('Validation Passed', str(summary.get('validation_passed', summary.get('deployment_ready', 'NO')) or 'NO'))
    col_four.metric('Blocking Issues', 'None' if blocker_count == 0 else str(blocker_count))
    st.info(plain_summary)
    blocker_frame = build_blocker_frame(summary)
    st.markdown('### Top 3 Fixes')
    for idx, action in enumerate(build_top_fix_actions(summary), start=1):
        st.markdown(f'{idx}. {action}')
    st.markdown('### Blocker Groups')
    st.dataframe(_arrow_safe_frame(blocker_frame[['severity', 'headline', 'plain_english', 'fix']]), width='stretch', hide_index=True)
    if blockers:
        with st.expander('Technical blockers'):
            st.code(blockers)

    st.markdown('### Validation Gates')
    gates = pd.DataFrame([
        {'gate': 'Trade count', 'rule': '150-200 trades'},
        {'gate': 'Expectancy', 'rule': '> 0'},
        {'gate': 'Profit factor', 'rule': '> 1.30'},
        {'gate': 'Duplicate trades', 'rule': '= 0'},
        {'gate': 'Max drawdown', 'rule': '<= configured limit'},
    ])
    st.dataframe(_arrow_safe_frame(gates), width='stretch', hide_index=True)

    st.markdown('### Backtest Metrics')
    metrics_frame = _arrow_safe_frame(pd.DataFrame([
        {'metric': 'Total Trades', 'value': _safe_int(summary.get('total_trades', summary.get('closed_trades', 0)))},
        {'metric': 'Wins', 'value': _safe_int(summary.get('wins'))},
        {'metric': 'Losses', 'value': _safe_int(summary.get('losses'))},
        {'metric': 'Win Rate', 'value': round(_safe_float(summary.get('win_rate')), 2)},
        {'metric': 'Avg Win', 'value': round(_safe_float(summary.get('avg_win')), 2)},
        {'metric': 'Avg Loss', 'value': round(_safe_float(summary.get('avg_loss')), 2)},
        {'metric': 'Total PnL', 'value': round(_safe_float(summary.get('total_pnl', summary.get('pnl'))), 2)},
        {'metric': 'Expectancy/Trade', 'value': round(_safe_float(summary.get('expectancy_per_trade')), 2)},
        {'metric': 'Profit Factor', 'value': round(_safe_float(summary.get('profit_factor')), 2)},
        {'metric': 'Max Drawdown %', 'value': round(_safe_float(summary.get('max_drawdown_pct')), 2)},
    ]))
    st.dataframe(metrics_frame, width='stretch', hide_index=True)
    st.markdown('### Validation Snapshot')
    st.dataframe(_build_validation_snapshot(summary), width='stretch', hide_index=True)
    _render_quality_ladder(summary)
    if str(summary.get('deployment_ready', 'NO') or 'NO').upper() == 'YES':
        st.success('PASS: eligible for paper or live consideration, subject to operator approval.')
    else:
        st.warning('FAIL: remain paper-only until every validation blocker is cleared.')


def _render_execution_tab(*, status: str, broker_status: str, todays_trades: int, summary: dict[str, object]) -> None:
    st.markdown('### Execution Status')
    col_one, col_two, col_three = st.columns(3)
    col_one.metric('Current Status', status)
    col_two.metric('Broker Status', broker_status)
    col_three.metric("Today's Trades", todays_trades)

    st.markdown('### Discipline Metrics')
    discipline = _arrow_safe_frame(pd.DataFrame([
        {'metric': 'Duplicate rejections', 'value': _safe_int(summary.get('duplicate_rejections'))},
        {'metric': 'Risk-rule rejections', 'value': _safe_int(summary.get('risk_rule_rejections'))},
        {'metric': 'Avg trades/day', 'value': round(_safe_float(summary.get('avg_trades_per_day')), 2)},
        {'metric': 'Deployment Ready', 'value': str(summary.get('deployment_ready', 'NO') or 'NO')},
    ]))
    st.dataframe(discipline, width='stretch', hide_index=True)

    st.markdown('### Recent Executions')
    executions = _safe_tail_dataframe(EXECUTED_TRADES_OUTPUT, ['timestamp', 'strategy', 'symbol', 'side', 'quantity', 'entry', 'status', 'broker_message'])
    st.dataframe(_arrow_safe_frame(executions), width='stretch', hide_index=True)

    st.markdown('### Log Previews')
    log_col_one, log_col_two = st.columns(2)
    with log_col_one:
        with st.expander('Execution Log'):
            st.markdown(f"```text\n{_safe_log_preview(EXECUTION_LOG)}\n```")
        with st.expander('App Log'):
            st.markdown(f"```text\n{_safe_log_preview(APP_LOG)}\n```")
    with log_col_two:
        with st.expander('Rejection Log'):
            st.markdown(f"```text\n{_safe_log_preview(REJECTIONS_LOG)}\n```")
        with st.expander('Recent Rejections Table'):
            rejections = _safe_tail_dataframe(REJECTIONS_LOG, ['timestamp', 'rejection_reason', 'rejection_category', 'rejection_detail'])
            st.dataframe(_arrow_safe_frame(rejections), width='stretch', hide_index=True)


def _render_tabs(*, strategy: str, symbol: str, timeframe: str, period: str, broker_choice: str, status: str, broker_status: str, trades: list[dict[str, object]], candles: pd.DataFrame, active_summary: dict[str, object], scorecard_summary: dict[str, object], todays_trades: int, market_data_summary: dict[str, object] | None = None) -> None:
    dashboard_tab, score_report_tab, zone_heatmap_tab, charts_tab, validation_tab, trades_tab, execution_tab = st.tabs(['Dashboard', 'Score Backtest Report', 'Zone Heatmap', 'Charts', 'Validation', 'Trades', 'Execution Logs'])
    with dashboard_tab:
        _render_dashboard_tab(strategy=strategy, symbol=symbol, timeframe=timeframe, period=period, broker_choice=broker_choice, status=status, broker_status=broker_status, trades=trades, active_summary=active_summary, scorecard_summary=scorecard_summary, todays_trades=todays_trades, candles=candles, market_data_summary=market_data_summary)
    with score_report_tab:
        _render_score_backtest_report_tab(trades=trades, summary=scorecard_summary)
    with zone_heatmap_tab:
        _render_zone_heatmap_tab(candles=candles, symbol=symbol)
    with charts_tab:
        _render_charts_tab(candles=candles, symbol=symbol, timeframe=timeframe)
    with validation_tab:
        _render_validation_tab(scorecard_summary)
    with trades_tab:
        _render_trades_tab(trades=trades, status=status)
    with execution_tab:
        _render_execution_tab(status=status, broker_status=broker_status, todays_trades=todays_trades, summary=scorecard_summary)


def main() -> None:
    _ensure_output_files()
    _minimal_theme()

    control_col_1, control_col_2, control_col_3 = st.columns(3)
    with control_col_1:
        symbol = st.text_input('Symbol', value=DEFAULT_SYMBOL)
        strategy = st.selectbox('Strategy', STRATEGY_OPTIONS)
        broker_choice = st.selectbox('Broker', BROKER_OPTIONS)
    with control_col_2:
        timeframe = st.selectbox('Timeframe', TIMEFRAME_OPTIONS, index=TIMEFRAME_OPTIONS.index(DEFAULT_INTERVAL) if DEFAULT_INTERVAL in TIMEFRAME_OPTIONS else 1)
        capital = st.number_input('Capital', min_value=1000.0, value=OPERATOR_DEFAULTS.capital, step=1000.0)
        risk_pct = st.number_input('Risk %', min_value=0.1, value=OPERATOR_DEFAULTS.risk_pct, step=0.1)
    with control_col_3:
        rr_ratio = st.number_input('RR Ratio', min_value=1.0, value=OPERATOR_DEFAULTS.rr_ratio, step=0.1)
        mode = st.selectbox('Mode', MODE_OPTIONS, index=MODE_OPTIONS.index(OPERATOR_DEFAULTS.mode) if OPERATOR_DEFAULTS.mode in MODE_OPTIONS else 0)
        period = period_for_interval(timeframe)
        st.caption(f'Fetch window: {period}')
        action_row = st.columns(2)
        run_clicked = _button_clicked(action_row[0], 'Start Paper', legacy_label='Run', type='primary', width='stretch')
        backtest_clicked = _button_clicked(action_row[1], 'Run Backtest', legacy_label='Backtest', width='stretch')

    normalized_symbol = symbol.strip() or DEFAULT_SYMBOL
    resting_summary = dict(st.session_state.get('backtest_summary', {}) or {})
    if not run_clicked and not backtest_clicked:
        _render_header(strategy, normalized_symbol, timeframe, mode, broker_choice, resting_summary)
        _render_tabs(strategy=strategy, symbol=normalized_symbol, timeframe=timeframe, period=period_for_interval(timeframe), broker_choice=broker_choice, status='Ready', broker_status='Paper broker active', trades=[], candles=pd.DataFrame(), active_summary={}, scorecard_summary=resting_summary, todays_trades=0, market_data_summary={})
        return

    try:
        request = _build_request(strategy, normalized_symbol, timeframe, float(capital), float(risk_pct), float(rr_ratio), mode, broker_choice, run_clicked, backtest_clicked)
        result = run_operator_action(request)
        summary = dict(result.backtest_summary or result.active_summary or {})
        _render_header(strategy, normalized_symbol, timeframe, mode, broker_choice, summary)
        if _result_failed(result.status):
            st.session_state.pop('backtest_summary', None)
            _append_text_log(APP_LOG, result.status)
            _append_text_log(ERRORS_LOG, result.status)
            _render_tabs(strategy=strategy, symbol=normalized_symbol, timeframe=timeframe, period=result.period, broker_choice=broker_choice, status=result.status, broker_status=result.broker_status, trades=result.trades, candles=result.candles, active_summary=result.active_summary, scorecard_summary=summary, todays_trades=result.todays_trades, market_data_summary=result.market_data_summary)
            _render_execution_feedback(result.execution_messages)
            st.error(result.status)
            return

        if run_clicked:
            st.session_state.pop('backtest_summary', None)
            _append_text_log(APP_LOG, f'EXECUTION completed for {strategy} {normalized_symbol} broker={broker_choice}')
        else:
            st.session_state['backtest_summary'] = result.backtest_summary
            _append_text_log(APP_LOG, f'BACKTEST completed for {strategy} {normalized_symbol} {timeframe}')

        _append_text_log(APP_LOG, result.status)
        _render_tabs(strategy=strategy, symbol=normalized_symbol, timeframe=timeframe, period=result.period, broker_choice=broker_choice, status=result.status, broker_status=result.broker_status, trades=result.trades, candles=result.candles, active_summary=result.active_summary, scorecard_summary=summary, todays_trades=result.todays_trades, market_data_summary=result.market_data_summary)
        _render_execution_feedback(result.execution_messages)
    except Exception as exc:
        message = f'Trading UI failure: {exc}'
        _append_text_log(APP_LOG, message)
        _append_text_log(ERRORS_LOG, message)
        append_log(message)
        st.error(f'Run failed: {exc}')


if __name__ == '__main__':
    main()






