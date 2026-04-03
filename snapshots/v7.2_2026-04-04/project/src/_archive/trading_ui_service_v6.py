from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.reporting_service import recent_trade_summary, safe_float, safe_int, short_broker_status
from src.runtime_file_service import append_text_log, ensure_output_files
from src.runtime_models import TradingActionRequest


def initialize_ui_runtime(output_paths: list[Path], log_paths: list[Path]) -> None:
    ensure_output_files(output_paths, log_paths)


def log_ui_event(path: Path, message: str) -> None:
    append_text_log(path, message)


def apply_minimal_theme() -> None:
    st.set_page_config(page_title='Trading Desk V4', page_icon='chart', layout='wide')
    st.markdown(
        '''
        <style>
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, rgba(34, 197, 94, 0.08), transparent 28%),
                radial-gradient(circle at top right, rgba(56, 189, 248, 0.08), transparent 24%),
                linear-gradient(180deg, #071019 0%, #0b1724 55%, #0a1320 100%);
        }
        .main .block-container {
            max-width: 1180px;
            padding-top: 1.25rem;
            padding-bottom: 2rem;
        }
        [data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(15, 23, 42, 0.96), rgba(17, 24, 39, 0.92));
            border: 1px solid rgba(148, 163, 184, 0.14);
            border-radius: 18px;
            padding: 10px;
            box-shadow: 0 14px 28px rgba(2, 6, 23, 0.18);
        }
        .desk-card {
            background: linear-gradient(180deg, rgba(15, 23, 42, 0.92), rgba(15, 23, 42, 0.82));
            border: 1px solid rgba(148, 163, 184, 0.14);
            border-radius: 22px;
            padding: 18px;
            margin-bottom: 14px;
            box-shadow: 0 18px 40px rgba(2, 6, 23, 0.2);
        }
        .desk-hero {
            padding: 26px 28px;
            border-radius: 26px;
            background:
                linear-gradient(135deg, rgba(15, 23, 42, 0.96), rgba(17, 24, 39, 0.88)),
                linear-gradient(90deg, rgba(34, 197, 94, 0.08), rgba(56, 189, 248, 0.08));
            border: 1px solid rgba(148, 163, 184, 0.14);
            margin-bottom: 16px;
            box-shadow: 0 18px 44px rgba(2, 6, 23, 0.26);
        }
        .desk-kicker {
            color: #38bdf8;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-size: 0.72rem;
            font-weight: 700;
            margin-bottom: 8px;
        }
        .desk-title {
            color: #f8fafc;
            font-size: 2rem;
            font-weight: 700;
            line-height: 1.1;
            margin: 0;
        }
        .desk-subtitle {
            color: #94a3b8;
            font-size: 0.98rem;
            margin: 10px 0 0 0;
        }
        .desk-chip {
            display: inline-block;
            padding: 7px 11px;
            margin: 8px 8px 0 0;
            border-radius: 999px;
            background: rgba(15, 23, 42, 0.9);
            border: 1px solid rgba(148, 163, 184, 0.16);
            color: #cbd5e1;
            font-size: 0.82rem;
        }
        .desk-section-title {
            color: #e2e8f0;
            font-size: 1rem;
            font-weight: 700;
            margin: 0 0 0.5rem 0;
        }
        .desk-panel-title {
            color: #e2e8f0;
            font-size: 1.05rem;
            font-weight: 700;
            margin-bottom: 0.35rem;
        }
        .desk-label {
            color:#94a3b8;
            font-size:0.82rem;
            margin-bottom:0.35rem;
            text-transform:uppercase;
            letter-spacing:0.04em;
        }
        .desk-value {color:#e2e8f0; font-size:1.02rem;}
        .desk-note {color:#cbd5e1; font-size:0.88rem; line-height:1.45;}
        </style>
        ''',
        unsafe_allow_html=True,
    )


def render_summary_cards(trades: list[dict[str, object]], summary: dict[str, object], todays_trades: int) -> None:
    total_trades = safe_int(summary.get('total_trades', 0))
    win_rate = safe_float(summary.get('win_rate', 0.0))
    pnl = safe_float(summary.get('total_pnl', summary.get('pnl', 0.0)))
    last_signal = str(trades[-1].get('side', 'NONE')) if trades else 'NONE'
    profit_factor = summary.get('profit_factor', 0.0)
    avg_win = safe_float(summary.get('avg_win', 0.0))
    avg_loss = safe_float(summary.get('avg_loss', 0.0))
    max_drawdown = safe_float(summary.get('max_drawdown', 0.0))

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


def render_operator_panels(status: str, trades: list[dict[str, object]], symbol: str, timeframe: str, period: str, broker_choice: str, broker_status: str) -> None:
    st.markdown('<div class="desk-card">', unsafe_allow_html=True)
    st.markdown('<div class="desk-label">Current Status</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="desk-value">{status}</div>', unsafe_allow_html=True)
    st.caption(f'Symbol={symbol} | Timeframe={timeframe} | Fetch window={period}')
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="desk-card">', unsafe_allow_html=True)
    st.markdown('<div class="desk-label">Broker Status</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="desk-value">{broker_choice} | {short_broker_status(broker_choice, broker_status)}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="desk-card">', unsafe_allow_html=True)
    st.markdown('<div class="desk-label">Recent Trade Summary</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="desk-value">{recent_trade_summary(trades)}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def build_request(strategy: str, symbol: str, timeframe: str, capital: float, risk_pct: float, rr_ratio: float, mode: str, broker_choice: str, run_clicked: bool, backtest_clicked: bool) -> TradingActionRequest:
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
