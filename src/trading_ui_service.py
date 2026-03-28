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
    st.set_page_config(page_title='Trading Desk V7', page_icon='chart', layout='wide')
    st.markdown(
        '''
        <style>
        :root {
            --desk-bg: #fff9f7;
            --desk-bg-soft: #fff3f0;
            --desk-card: rgba(255, 255, 255, 0.92);
            --desk-border: rgba(243, 114, 116, 0.14);
            --desk-text: #2d2f35;
            --desk-muted: #6b7280;
            --desk-accent: #ff7d87;
            --desk-accent-soft: #ffd6d9;
            --desk-blue: #6266f1;
            --desk-shadow: 0 20px 45px rgba(244, 141, 141, 0.12);
        }
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, rgba(255, 125, 135, 0.40), transparent 26%),
                radial-gradient(circle at 95% 40%, rgba(255, 125, 135, 0.34), transparent 22%),
                radial-gradient(circle at 80% 92%, rgba(255, 167, 173, 0.55), transparent 28%),
                linear-gradient(180deg, #fffdfc 0%, var(--desk-bg) 100%);
        }
        .main .block-container {
            max-width: 1220px;
            padding-top: 1rem;
            padding-bottom: 2rem;
        }
        [data-testid="stHeader"] {
            background: rgba(255,255,255,0.72);
        }
        [data-testid="stMetric"] {
            background: var(--desk-card);
            border: 1px solid rgba(255,255,255,0.65);
            border-radius: 22px;
            padding: 12px;
            box-shadow: var(--desk-shadow);
        }
        [data-testid="stMetricLabel"] {
            color: var(--desk-muted);
            font-weight: 600;
        }
        [data-testid="stMetricValue"] {
            color: var(--desk-text);
        }
        .desk-card {
            background: var(--desk-card);
            border: 1px solid var(--desk-border);
            border-radius: 24px;
            padding: 20px;
            margin-bottom: 14px;
            box-shadow: var(--desk-shadow);
            backdrop-filter: blur(6px);
        }
        .desk-label {
            color: var(--desk-muted);
            font-size: 0.78rem;
            margin-bottom: 0.35rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-weight: 700;
        }
        .desk-value {
            color: var(--desk-text);
            font-size: 1.02rem;
            line-height: 1.45;
        }
        .desk-note {
            color: var(--desk-muted);
            font-size: 0.92rem;
            line-height: 1.55;
        }
        .desk-chip {
            display: inline-block;
            padding: 9px 13px;
            margin: 10px 10px 0 0;
            border-radius: 999px;
            background: rgba(255,255,255,0.85);
            border: 1px solid rgba(243, 114, 116, 0.18);
            color: #444a57;
            font-size: 0.82rem;
            font-weight: 600;
        }
        .desk-panel-title {
            color: var(--desk-text);
            font-size: 1.02rem;
            font-weight: 700;
            margin-bottom: 0.4rem;
        }
        .desk-section-title {
            color: var(--desk-text);
            font-size: 1rem;
            font-weight: 700;
            margin: 0 0 0.55rem 0;
        }
        .desk-hero {
            position: relative;
            overflow: hidden;
            padding: 0;
            border-radius: 34px;
            background: linear-gradient(135deg, rgba(255,255,255,0.96), rgba(255,246,244,0.92));
            border: 1px solid rgba(243, 114, 116, 0.16);
            box-shadow: 0 30px 65px rgba(244, 141, 141, 0.14);
            margin-bottom: 18px;
        }
        .desk-hero-inner {
            display: grid;
            grid-template-columns: 1.2fr 0.8fr;
            gap: 16px;
            align-items: center;
            min-height: 340px;
            padding: 38px 38px 30px 38px;
        }
        .desk-hero-copy {
            position: relative;
            z-index: 2;
        }
        .desk-kicker {
            color: #ef5b68;
            text-transform: uppercase;
            letter-spacing: 0.14em;
            font-size: 0.72rem;
            font-weight: 800;
            margin-bottom: 12px;
        }
        .desk-title {
            color: var(--desk-text);
            font-size: 4rem;
            line-height: 0.95;
            letter-spacing: -0.05em;
            margin: 0;
            font-weight: 800;
        }
        .desk-subtitle {
            color: #5f6472;
            font-size: 1.12rem;
            line-height: 1.6;
            max-width: 680px;
            margin: 18px 0 0 0;
        }
        .desk-hero-art {
            position: relative;
            min-height: 250px;
            z-index: 1;
        }
        .desk-blob-a,
        .desk-blob-b,
        .desk-blob-c {
            position: absolute;
            border-radius: 999px;
            background: linear-gradient(135deg, #ff8c95, #ffb0b6);
            opacity: 0.95;
        }
        .desk-blob-a {
            width: 250px;
            height: 250px;
            top: -100px;
            left: -60px;
            border-bottom-left-radius: 120px;
            border-bottom-right-radius: 140px;
        }
        .desk-blob-b {
            width: 320px;
            height: 200px;
            right: -80px;
            bottom: -40px;
            border-top-left-radius: 180px;
            border-top-right-radius: 40px;
        }
        .desk-blob-c {
            width: 120px;
            height: 120px;
            right: 40px;
            top: 28px;
            opacity: 0.22;
            background: linear-gradient(135deg, #ff7d87, #ffd6d9);
        }
        .desk-monitor {
            position: absolute;
            right: 58px;
            top: 44px;
            width: 210px;
            height: 130px;
            border-radius: 16px;
            border: 6px solid #22252d;
            background: #fff;
            box-shadow: 0 20px 40px rgba(0,0,0,0.12);
        }
        .desk-monitor::after {
            content: '';
            position: absolute;
            left: 80px;
            bottom: -26px;
            width: 44px;
            height: 26px;
            background: #22252d;
            border-bottom-left-radius: 8px;
            border-bottom-right-radius: 8px;
        }
        .desk-candle {
            position: absolute;
            bottom: 28px;
            width: 12px;
            border-radius: 6px;
        }
        .desk-candle.green { background: #5cc16f; }
        .desk-candle.red { background: #ef6f79; }
        .desk-desk {
            position: absolute;
            right: 22px;
            bottom: 36px;
            width: 310px;
            height: 16px;
            background: #ff93a0;
            border-radius: 10px;
        }
        .desk-desk::before,
        .desk-desk::after {
            content: '';
            position: absolute;
            width: 12px;
            height: 110px;
            background: #1f2937;
            bottom: -108px;
            border-radius: 8px;
        }
        .desk-desk::before { left: 28px; }
        .desk-desk::after { right: 36px; }
        .desk-avatar {
            position: absolute;
            right: 110px;
            bottom: 38px;
            width: 84px;
            height: 150px;
        }
        .desk-avatar .head {
            position: absolute;
            width: 44px;
            height: 44px;
            border-radius: 50%;
            background: #d4a27a;
            top: 0;
            left: 20px;
        }
        .desk-avatar .body {
            position: absolute;
            width: 62px;
            height: 80px;
            border-radius: 22px;
            background: #6cb8e6;
            top: 36px;
            left: 12px;
        }
        .desk-avatar .leg-left,
        .desk-avatar .leg-right {
            position: absolute;
            width: 18px;
            height: 62px;
            background: #5b5b5b;
            bottom: 0;
            border-radius: 12px;
        }
        .desk-avatar .leg-left { left: 22px; }
        .desk-avatar .leg-right { right: 22px; }
        .desk-badge {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            margin-top: 18px;
            padding: 11px 16px;
            border-radius: 999px;
            background: #6266f1;
            color: #fff;
            font-weight: 700;
            box-shadow: 0 14px 30px rgba(98, 102, 241, 0.22);
        }
        .desk-badge-soft {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            margin-left: 10px;
            padding: 11px 16px;
            border-radius: 999px;
            background: rgba(255,255,255,0.9);
            color: var(--desk-text);
            font-weight: 700;
            border: 1px solid rgba(243,114,116,0.18);
        }
        [data-baseweb="tab-list"] {
            gap: 10px;
            padding: 6px;
            background: rgba(255,255,255,0.82);
            border-radius: 18px;
            border: 1px solid rgba(243,114,116,0.14);
            width: fit-content;
            box-shadow: var(--desk-shadow);
            margin-bottom: 12px;
        }
        [data-baseweb="tab"] {
            background: transparent !important;
            border-radius: 14px !important;
            color: #5f6472 !important;
            font-weight: 700 !important;
            padding: 10px 18px !important;
        }
        [aria-selected="true"][data-baseweb="tab"] {
            background: #6266f1 !important;
            color: #ffffff !important;
        }
        .stDataFrame, [data-testid="stExpander"], [data-testid="stAlert"] {
            background: rgba(255,255,255,0.82);
            border-radius: 18px;
        }
        @media (max-width: 980px) {
            .desk-hero-inner {
                grid-template-columns: 1fr;
                min-height: auto;
            }
            .desk-title {
                font-size: 3rem;
            }
            .desk-hero-art {
                min-height: 220px;
            }
        }
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
