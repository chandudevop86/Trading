from __future__ import annotations

import sys
from pathlib import Path

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
    BACKTEST_RESULTS_OUTPUT,
    BACKTEST_SUMMARY_OUTPUT,
    BACKTEST_TRADES_OUTPUT,
    BROKER_LOG,
    BROKER_OPTIONS,
    DEFAULT_INTERVAL,
    DEFAULT_SYMBOL,
    ERRORS_LOG,
    EXECUTED_TRADES_OUTPUT,
    EXECUTION_LOG,
    LIVE_LOG_OUTPUT,
    LIVE_OHLCV_OUTPUT,
    MODE_OPTIONS,
    OHLCV_OUTPUT,
    ORDER_HISTORY_OUTPUT,
    PAPER_LOG_OUTPUT,
    PAPER_ORDER_HISTORY_OUTPUT,
    REJECTIONS_LOG,
    SIGNAL_OUTPUT,
    STRATEGY_OPTIONS,
    TIMEFRAME_OPTIONS,
    TRADES_OUTPUT,
    runtime_log_paths,
    runtime_output_paths,
)
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.runtime_strategy_presets import OPERATOR_DEFAULTS
from src.runtime_strategy_registry import configure_runtime_strategy_dependencies, run_configured_runtime_strategy
from src.strike_selector import attach_option_strikes
from src.trading_core import append_log, configure_file_logging
from src.runtime_models import period_for_interval
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
    initialize_ui_runtime(
        runtime_output_paths(),
        runtime_log_paths(),
    )


def _minimal_theme() -> None:
    apply_minimal_theme()


def _render_summary_cards(trades: list[dict[str, object]], summary: dict[str, object], todays_trades: int) -> None:
    render_summary_cards(trades, summary, todays_trades)


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
    trade_quality_fix = 'Run a 100-200 trade backtest and keep avg trades/day near 1.'
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
        validation_fix = 'Keep validation only in the 100-200 trade window before considering deployment.'
    elif validation_available and not deployment_ready:
        blockers = str(summary.get('deployment_blockers', '') or '').strip() or 'validation gates not passed'
        validation_issue = blockers
        validation_fix = 'Do not promote live deployment until every blocker is cleared.'
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
        {
            'area': 'Trade Quality',
            'score': round(min(trade_quality_score, 10.0), 1),
            'current issue': trade_quality_issue,
            'exact next fix': trade_quality_fix,
        },
        {
            'area': 'Validation Metrics',
            'score': round(min(validation_score, 10.0), 1),
            'current issue': validation_issue,
            'exact next fix': validation_fix,
        },
        {
            'area': 'Execution Discipline',
            'score': round(min(execution_score, 10.0), 1),
            'current issue': execution_issue,
            'exact next fix': execution_fix,
        },
    ]



def _scorecard_detail_map(summary: dict[str, object], *, status: str, todays_trades: int, strategy_label: str) -> dict[str, list[str]]:
    blockers = str(summary.get('deployment_blockers', '') or '').strip() or 'None'
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
            f"Blockers: {blockers}",
        ],
        'Execution Discipline': [
            f"Duplicate rejections: {_safe_int(summary.get('duplicate_rejections'))}",
            f"Risk-rule rejections: {_safe_int(summary.get('risk_rule_rejections'))}",
            f"Status: {status}",
            f"Validation passed: {str(summary.get('validation_passed', summary.get('deployment_ready', 'NO')) or 'NO')}",
        ],
    }
def _render_scorecard(summary: dict[str, object], status: str, todays_trades: int, strategy_label: str) -> None:
    rows = _build_scorecard_rows(summary, status=status, todays_trades=todays_trades)
    details = _scorecard_detail_map(summary, status=status, todays_trades=todays_trades, strategy_label=strategy_label)
    st.markdown('### Current-State Scorecard')
    st.caption('Green = strong, amber = watchlist, red = needs action.')
    st.dataframe(_scorecard_styler(rows), use_container_width=True, hide_index=True)
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

def main() -> None:
    _ensure_output_files()
    _minimal_theme()
    st.markdown(
        '<div class="desk-card"><h2 style="margin:0;color:#e2e8f0;">Production Trading Desk</h2><p style="margin:8px 0 0 0;color:#94a3b8;">Minimal operator controls with runtime orchestration delegated to legacy services.</p></div>',
        unsafe_allow_html=True,
    )

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
        st.markdown('<div class="desk-label">Run</div>', unsafe_allow_html=True)
        run_clicked = action_row[0].button('Run', type='primary', use_container_width=True)
        st.markdown('<div class="desk-label">Backtest</div>', unsafe_allow_html=True)
        backtest_clicked = action_row[1].button('Backtest', use_container_width=True)

    normalized_symbol = symbol.strip() or DEFAULT_SYMBOL
    if not run_clicked and not backtest_clicked:
        resting_summary = dict(st.session_state.get('backtest_summary', {}) or {})
        _render_summary_cards([], {}, 0)
        _render_operator_panels('Ready', [], normalized_symbol, timeframe, period_for_interval(timeframe), broker_choice, 'Paper broker active')
        _render_scorecard(resting_summary, 'Ready', 0, strategy)
        return

    try:
        request = _build_request(strategy, normalized_symbol, timeframe, float(capital), float(risk_pct), float(rr_ratio), mode, broker_choice, run_clicked, backtest_clicked)
        result = run_operator_action(request)
        if _result_failed(result.status):
            st.session_state.pop('backtest_summary', None)
            _append_text_log(APP_LOG, result.status)
            _append_text_log(ERRORS_LOG, result.status)
            _render_summary_cards(result.trades, result.active_summary, result.todays_trades)
            _render_operator_panels(result.status, result.trades, normalized_symbol, timeframe, result.period, broker_choice, result.broker_status)
            _render_scorecard(dict(result.backtest_summary or result.active_summary or {}), result.status, result.todays_trades, strategy)
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
        _render_summary_cards(result.trades, result.active_summary, result.todays_trades)
        _render_operator_panels(result.status, result.trades, normalized_symbol, timeframe, result.period, broker_choice, result.broker_status)
        _render_scorecard(dict(result.backtest_summary or result.active_summary or {}), result.status, result.todays_trades, strategy)
        _render_execution_feedback(result.execution_messages)
    except Exception as exc:
        message = f'Trading UI failure: {exc}'
        _append_text_log(APP_LOG, message)
        _append_text_log(ERRORS_LOG, message)
        append_log(message)
        st.error(f'Run failed: {exc}')


if __name__ == '__main__':
    main()















