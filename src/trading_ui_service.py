from __future__ import annotations

from pathlib import Path

import streamlit as st

from src.reporting_service import recent_trade_summary, safe_float, safe_int, short_broker_status
from src.runtime_file_service import append_text_log, ensure_output_files
from src.runtime_models import TradingActionRequest
from src.volatility_filter import latest_volatility_snapshot


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
            --desk-bg: #f4f5ee;
            --desk-bg-soft: #ecefdf;
            --desk-card: rgba(255, 255, 255, 0.90);
            --desk-border: rgba(112, 128, 96, 0.14);
            --desk-text: #283128;
            --desk-muted: #586255;
            --desk-accent: #7a8b5a;
            --desk-accent-soft: #d4dcc3;
            --desk-blue: #5a6f57;
            --desk-shadow: 0 20px 45px rgba(96, 112, 82, 0.10);
        }
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, rgba(122, 139, 90, 0.12), transparent 24%),
                radial-gradient(circle at 92% 38%, rgba(167, 180, 135, 0.10), transparent 20%),
                radial-gradient(circle at 78% 90%, rgba(212, 220, 195, 0.14), transparent 24%),
                linear-gradient(180deg, #fafbf7 0%, var(--desk-bg) 100%);
        }
        .main .block-container {
            max-width: 1220px;
            padding-top: 1rem;
            padding-bottom: 2rem;
        }
        [data-testid="stHeader"] {
            background: rgba(249,250,246,0.76);
        }
        [data-testid="stMetric"] {
            background: var(--desk-card);
            border: 1px solid rgba(255,255,255,0.65);
            border-radius: 22px;
            padding: 12px;
            box-shadow: var(--desk-shadow);
        }
        [data-testid="stMetricLabel"] {
            color: #55604f;
            font-weight: 600;
        }
        [data-testid="stMetricValue"] {
            color: var(--desk-text);
        }
        [data-testid="stTextInputRootElement"] input,
        [data-testid="stNumberInput"] input,
        [data-testid="stSelectbox"] [data-baseweb="select"] > div {
            border: 1px solid rgba(122, 139, 90, 0.18) !important;
            box-shadow: none !important;
        }
        [data-testid="stNumberInput"] button {
            color: #5a6f57 !important;
        }
        div[data-testid="stButton"] > button[kind="primary"] {
            background: linear-gradient(135deg, #7a8b5a, #8fa06c) !important;
            color: #f8fafc !important;
            border: 1px solid rgba(90, 111, 87, 0.30) !important;
            box-shadow: 0 12px 24px rgba(90, 111, 87, 0.16) !important;
        }
        div[data-testid="stButton"] > button[kind="primary"]:hover {
            background: linear-gradient(135deg, #6f8150, #879967) !important;
        }
        div[data-testid="stButton"] > button:not([kind="primary"]) {
            background: rgba(255, 255, 255, 0.88) !important;
            color: #40503d !important;
            border: 1px solid rgba(122, 139, 90, 0.20) !important;
        }
        div[data-testid="stButton"] > button:not([kind="primary"]):hover {
            background: rgba(244, 246, 238, 0.96) !important;
            border-color: rgba(122, 139, 90, 0.30) !important;
        }
        label, .stTextInput label, .stSelectbox label, .stNumberInput label {
            color: #4f5d49 !important;
            font-weight: 700 !important;
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
            border: 1px solid rgba(123, 143, 85, 0.18);
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
            background: linear-gradient(135deg, rgba(255,255,252,0.96), rgba(241,244,231,0.94));
            border: 1px solid rgba(123, 143, 85, 0.16);
            box-shadow: 0 30px 65px rgba(92, 112, 74, 0.12);
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
            color: #6f8451;
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
            background: linear-gradient(135deg, #8a9b62, #b7c48c);
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
            background: linear-gradient(135deg, #a6b57a, #dde6c4);
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
            background: #9caf6d;
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


def render_summary_cards(trades: list[dict[str, object]], summary: dict[str, object], todays_trades: int, candles: pd.DataFrame | None = None) -> None:
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

    snapshot = latest_volatility_snapshot(candles if candles is not None else pd.DataFrame())
    score = int(snapshot.get('volatility_score', 0) or 0)
    market_state = str(snapshot.get('market_state', 'QUIET') or 'QUIET')
    decision = 'TRADE ALLOWED' if bool(snapshot.get('trade_allowed')) else str(snapshot.get('volatility_decision', 'NO_TRADE_LOW_VOL') or 'NO_TRADE_LOW_VOL').replace('_', ' ')
    badge = '?' if bool(snapshot.get('trade_allowed')) else '??'
    st.markdown(
        (
            '<div class="desk-card" style="margin-top:12px;">'
            '<div class="desk-panel-title">Volatility Filter</div>'
            f'<div class="desk-note" style="margin-bottom:10px;">Volatility Score: <strong>{score} / 10</strong> | Market State: <strong>{market_state}</strong></div>'
            f'<div class="desk-note">ATR%: {float(snapshot.get("atr_pct", 0.0)):.2f}% | Open Vol: {float(snapshot.get("opening_volatility_pct", 0.0)):.2f}% | VWAP Dev: {float(snapshot.get("vwap_deviation_pct", 0.0)):.2f}% | Expansion: {float(snapshot.get("expansion_ratio", 0.0)):.2f}x</div>'
            f'<div class="desk-note" style="margin-top:8px;">Decision: {badge} <strong>{decision}</strong></div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )


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
