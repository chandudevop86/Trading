from __future__ import annotations

import os
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
from src.demand_supply_bot import generate_trades as generate_demand_supply_trades
from src.indicator_bot import generate_indicator_rows
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.strike_selector import attach_option_strikes
from src.trading_core import append_log, configure_file_logging
from src.trading_runtime_service import latest_actionable_trades, period_for_interval, run_operator_action
from src.trading_ui_service import apply_minimal_theme, build_request, initialize_ui_runtime, log_ui_event, render_operator_panels, render_summary_cards

DATA_DIR = Path('data')
LOG_DIR = Path('logs')
OHLCV_OUTPUT = DATA_DIR / 'ohlcv.csv'
LIVE_OHLCV_OUTPUT = DATA_DIR / 'live_ohlcv.csv'
TRADES_OUTPUT = DATA_DIR / 'trades.csv'
SIGNAL_OUTPUT = DATA_DIR / 'output.csv'
EXECUTED_TRADES_OUTPUT = DATA_DIR / 'executed_trades.csv'
PAPER_LOG_OUTPUT = DATA_DIR / 'paper_trading_logs_all.csv'
LIVE_LOG_OUTPUT = DATA_DIR / 'live_trading_logs_all.csv'
BACKTEST_TRADES_OUTPUT = DATA_DIR / 'backtest_trades.csv'
BACKTEST_SUMMARY_OUTPUT = DATA_DIR / 'backtest_summary.csv'
BACKTEST_RESULTS_OUTPUT = DATA_DIR / 'backtest_results_all.csv'
ORDER_HISTORY_OUTPUT = DATA_DIR / 'order_history.csv'
PAPER_ORDER_HISTORY_OUTPUT = DATA_DIR / 'paper_order_history.csv'
APP_LOG = LOG_DIR / 'app.log'
BROKER_LOG = LOG_DIR / 'broker.log'
EXECUTION_LOG = LOG_DIR / 'execution.log'
REJECTIONS_LOG = LOG_DIR / 'rejections.log'
ERRORS_LOG = LOG_DIR / 'errors.log'
DEFAULT_SYMBOL = os.getenv('TRADING_SYMBOL', '^NSEI').strip() or '^NSEI'
DEFAULT_INTERVAL = os.getenv('TRADING_INTERVAL', '5m').strip() or '5m'
TIMEFRAME_OPTIONS = ['1m', '5m', '15m', '30m', '1h', '1d']
STRATEGY_OPTIONS = ['Breakout', 'Demand Supply', 'Indicator', 'One Trade/Day', 'MTF 5m', 'AMD + FVG + Supply/Demand']
BROKER_OPTIONS = ['Paper', 'Dhan Live']

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
        [
            OHLCV_OUTPUT,
            LIVE_OHLCV_OUTPUT,
            TRADES_OUTPUT,
            SIGNAL_OUTPUT,
            EXECUTED_TRADES_OUTPUT,
            PAPER_LOG_OUTPUT,
            LIVE_LOG_OUTPUT,
            BACKTEST_TRADES_OUTPUT,
            BACKTEST_SUMMARY_OUTPUT,
            BACKTEST_RESULTS_OUTPUT,
            ORDER_HISTORY_OUTPUT,
            PAPER_ORDER_HISTORY_OUTPUT,
        ],
        [APP_LOG, BROKER_LOG, EXECUTION_LOG, REJECTIONS_LOG, ERRORS_LOG],
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


def _latest_actionable_trades(trades: list[dict[str, object]]) -> list[dict[str, object]]:
    return latest_actionable_trades(trades)


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
        capital = st.number_input('Capital', min_value=1000.0, value=20000.0, step=1000.0)
        risk_pct = st.number_input('Risk %', min_value=0.1, value=1.0, step=0.1)
    with control_col_3:
        rr_ratio = st.number_input('RR Ratio', min_value=1.0, value=2.0, step=0.1)
        mode = st.selectbox('Mode', ['Conservative', 'Balanced', 'Aggressive'], index=1)
        period = period_for_interval(timeframe)
        st.caption(f'Fetch window: {period}')
        action_row = st.columns(2)
        st.markdown('<div class="desk-label">Run</div>', unsafe_allow_html=True)
        run_clicked = action_row[0].button('Run', type='primary', use_container_width=True)
        st.markdown('<div class="desk-label">Backtest</div>', unsafe_allow_html=True)
        backtest_clicked = action_row[1].button('Backtest', use_container_width=True)

    normalized_symbol = symbol.strip() or DEFAULT_SYMBOL
    if not run_clicked and not backtest_clicked:
        _render_summary_cards([], {}, 0)
        _render_operator_panels('Ready', [], normalized_symbol, timeframe, period_for_interval(timeframe), broker_choice, 'Paper broker active')
        return

    try:
        request = _build_request(strategy, normalized_symbol, timeframe, float(capital), float(risk_pct), float(rr_ratio), mode, broker_choice, run_clicked, backtest_clicked)
        result = run_operator_action(request)
        if run_clicked:
            st.session_state.pop('backtest_summary', None)
            _append_text_log(APP_LOG, f'EXECUTION completed for {strategy} {normalized_symbol} broker={broker_choice}')
        else:
            st.session_state['backtest_summary'] = result.backtest_summary
            _append_text_log(APP_LOG, f'BACKTEST completed for {strategy} {normalized_symbol} {timeframe}')

        _append_text_log(APP_LOG, result.status)
        _render_summary_cards(result.trades, result.active_summary, result.todays_trades)
        _render_operator_panels(result.status, result.trades, normalized_symbol, timeframe, result.period, broker_choice, result.broker_status)
    except Exception as exc:
        message = f'Trading UI failure: {exc}'
        _append_text_log(APP_LOG, message)
        _append_text_log(ERRORS_LOG, message)
        append_log(message)
        st.error(f'Run failed: {exc}')


if __name__ == '__main__':
    main()
