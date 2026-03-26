from __future__ import annotations

import math
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.trading_core import append_log, configure_file_logging
from src.trading_runtime_service import (
    TradingActionRequest,
    fetch_ohlcv_data,
    latest_actionable_trades,
    period_for_interval,
    run_operator_action,
    run_strategy,
)

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


def _append_text_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with path.open('a', encoding='utf-8') as handle:
        handle.write(f'[{stamp}] {message}\n')


def _ensure_output_files() -> None:
    for path in [
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
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            pd.DataFrame().to_csv(path, index=False)
    for path in [APP_LOG, BROKER_LOG, EXECUTION_LOG, REJECTIONS_LOG, ERRORS_LOG]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == '':
            return default
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == '':
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _recent_trade_summary(trades: list[dict[str, object]]) -> str:
    if not trades:
        return 'No recent trade generated.'
    last = dict(trades[-1])
    return (
        f"{last.get('side', 'NA')} {last.get('strategy', 'TRADE')} | "
        f"Entry {float(last.get('entry', last.get('entry_price', 0.0)) or 0.0):.2f} | "
        f"SL {float(last.get('stop_loss', 0.0) or 0.0):.2f} | "
        f"Target {float(last.get('target', last.get('target_price', 0.0)) or 0.0):.2f} | "
        f"Score {float(last.get('score', 0.0) or 0.0):.2f}"
    )


def _short_broker_status(broker_choice: str, broker_status: str) -> str:
    if broker_choice == 'Dhan Live':
        return 'Dhan live active' if 'armed' in broker_status.lower() else broker_status
    return 'Paper broker active'


def _minimal_theme() -> None:
    st.set_page_config(page_title='Trading Desk', page_icon='chart', layout='wide')
    st.markdown(
        '''
        <style>
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(180deg, #08111a 0%, #0b1724 100%);
        }
        .main .block-container {max-width: 960px; padding-top: 1.75rem;}
        [data-testid="stMetric"] {
            background: rgba(15, 23, 42, 0.92);
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 14px;
            padding: 8px;
        }
        .desk-card {
            background: rgba(15, 23, 42, 0.9);
            border: 1px solid rgba(148, 163, 184, 0.16);
            border-radius: 18px;
            padding: 16px;
            margin-bottom: 14px;
        }
        .desk-label {color:#94a3b8; font-size:0.86rem; margin-bottom:0.35rem;}
        .desk-value {color:#e2e8f0; font-size:1.02rem;}
        </style>
        ''',
        unsafe_allow_html=True,
    )


def _render_summary_cards(trades: list[dict[str, object]], summary: dict[str, object], todays_trades: int) -> None:
    total_trades = _safe_int(summary.get('total_trades', 0))
    win_rate = _safe_float(summary.get('win_rate', 0.0))
    pnl = _safe_float(summary.get('total_pnl', summary.get('pnl', 0.0)))
    last_signal = str(trades[-1].get('side', 'NONE')) if trades else 'NONE'
    profit_factor = summary.get('profit_factor', 0.0)
    avg_win = _safe_float(summary.get('avg_win', 0.0))
    avg_loss = _safe_float(summary.get('avg_loss', 0.0))
    max_drawdown = _safe_float(summary.get('max_drawdown', 0.0))

    row_one = st.columns(5)
    row_one[0].metric('Total Trades', str(total_trades))
    row_one[1].metric('Win Rate', f'{win_rate:.2f}%')
    row_one[2].metric('PnL', f'{pnl:.2f}')
    row_one[3].metric('Last Signal', last_signal)
    row_one[4].metric('Profit Factor', str(profit_factor))

    row_two = st.columns(4)
    row_two[0].metric('Avg Win', f'{avg_win:.2f}')
    row_two[1].metric('Avg Loss', f'{avg_loss:.2f}')
    row_two[2].metric('Max Drawdown', f'{max_drawdown:.2f}')
    row_two[3].metric("Today's Trades", str(todays_trades))


def _render_operator_panels(status: str, trades: list[dict[str, object]], symbol: str, timeframe: str, period: str, broker_choice: str, broker_status: str) -> None:
    st.markdown('<div class="desk-card">', unsafe_allow_html=True)
    st.markdown('<div class="desk-label">Current Status</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="desk-value">{status}</div>', unsafe_allow_html=True)
    st.caption(f'Symbol={symbol} | Timeframe={timeframe} | Fetch window={period}')
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="desk-card">', unsafe_allow_html=True)
    st.markdown('<div class="desk-label">Broker Status</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="desk-value">{broker_choice} | {_short_broker_status(broker_choice, broker_status)}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="desk-card">', unsafe_allow_html=True)
    st.markdown('<div class="desk-label">Recent Trade Summary</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="desk-value">{_recent_trade_summary(trades)}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def _build_request(strategy: str, symbol: str, timeframe: str, capital: float, risk_pct: float, rr_ratio: float, mode: str, broker_choice: str, run_clicked: bool, backtest_clicked: bool) -> TradingActionRequest:
    return TradingActionRequest(
        strategy=strategy,
        symbol=symbol,
        timeframe=timeframe,
        capital=float(capital),
        risk_pct=float(risk_pct),
        rr_ratio=float(rr_ratio),
        mode=mode,
        broker_choice=broker_choice,
        run_requested=bool(run_clicked),
        backtest_requested=bool(backtest_clicked),
    )


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
