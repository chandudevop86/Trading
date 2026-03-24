from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from src.amd_fvg_sd_bot import ConfluenceConfig, generate_trades as generate_amd_fvg_sd_trades
from src.backtest_engine import BacktestConfig, run_backtest
from src.breakout_bot import Candle, generate_trades as generate_breakout_trades
from src.demand_supply_bot import generate_trades as generate_demand_supply_trades
from src.indicator_bot import IndicatorConfig, generate_indicator_rows
from src.live_ohlcv import fetch_live_ohlcv
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.strike_selector import attach_option_strikes
from src.trading_core import append_log, configure_file_logging, prepare_trading_data, write_rows
from src.strategy_service import StrategyContext, run_strategy_workflow

DATA_DIR = Path('data')
OHLCV_OUTPUT = DATA_DIR / 'ohlcv.csv'
TRADES_OUTPUT = DATA_DIR / 'trades.csv'
BACKTEST_OUTPUT = DATA_DIR / 'backtest_results.csv'
DEFAULT_SYMBOL = os.getenv('TRADING_SYMBOL', '^NSEI').strip() or '^NSEI'
DEFAULT_INTERVAL = os.getenv('TRADING_INTERVAL', '5m').strip() or '5m'
DEFAULT_PERIOD = os.getenv('TRADING_PERIOD', '5d').strip() or '5d'

configure_file_logging()


def _df_to_candles(df: pd.DataFrame) -> list[Candle]:
    prepared = prepare_trading_data(df)
    candles: list[Candle] = []
    for row in prepared.itertuples(index=False):
        candles.append(
            Candle(
                timestamp=pd.Timestamp(row.timestamp).to_pydatetime(),
                open=float(row.open),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
                volume=float(row.volume),
            )
        )
    return candles


def fetch_ohlcv_data(symbol: str, interval: str = DEFAULT_INTERVAL, period: str = DEFAULT_PERIOD) -> pd.DataFrame:
    rows = fetch_live_ohlcv(symbol, interval, period)
    return prepare_trading_data(pd.DataFrame(rows or []))


def _attach_option_metrics(rows: list[dict[str, object]], symbol: str, fetch_option_metrics: bool) -> list[dict[str, object]]:
    del symbol, fetch_option_metrics
    return [dict(row) for row in rows]


def _attach_indicator_trade_levels(rows: list[dict[str, object]], *, rr_ratio: float, trailing_sl_pct: float) -> list[dict[str, object]]:
    enriched: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        side = str(item.get('side', '')).upper()
        if side not in {'BUY', 'SELL'}:
            enriched.append(item)
            continue
        entry = float(item.get('entry_price', item.get('close', 0.0)) or 0.0)
        if entry <= 0:
            enriched.append(item)
            continue
        stop_loss = entry * (0.995 if side == 'BUY' else 1.005)
        target_price = entry + (entry - stop_loss) * float(rr_ratio) if side == 'BUY' else entry - (stop_loss - entry) * float(rr_ratio)
        item.setdefault('entry', entry)
        item.setdefault('entry_price', round(entry, 4))
        item.setdefault('stop_loss', round(stop_loss, 4))
        item.setdefault('trailing_stop_loss', round(stop_loss, 4))
        item.setdefault('trailing_sl_pct', round(float(trailing_sl_pct), 4))
        item.setdefault('target', round(target_price, 4))
        item.setdefault('target_price', round(target_price, 4))
        item.setdefault('score', float(item.get('score', 0.0) or 0.0))
        item.setdefault('reason', str(item.get('market_signal', 'INDICATOR_SIGNAL')))
        enriched.append(item)
    return enriched


def attach_lots(rows: list[dict[str, object]], lot_size: int, lots: int) -> list[dict[str, object]]:
    if lot_size <= 0 or lots <= 0:
        return rows
    qty = lot_size * lots
    output: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        item['lots'] = lots
        item['quantity'] = int(item.get('quantity', qty) or qty)
        output.append(item)
    return output


def run_strategy(*, strategy: str, candles: pd.DataFrame, capital: float, risk_pct: float, rr_ratio: float, trailing_sl_pct: float, symbol: str, strike_step: int, moneyness: str, strike_steps: int, fetch_option_metrics: bool, mtf_ema_period: int, mtf_setup_mode: str, mtf_retest_strength: bool, mtf_max_trades_per_day: int, amd_mode: str = 'Balanced', amd_swing_window: int = 3, amd_min_fvg_size: float = 0.35, amd_min_bvg_size: float = 0.25, amd_zone_fresh_bars: int = 24, amd_retest_tolerance_pct: float = 0.0015, amd_max_retest_bars: int = 6, amd_min_score_conservative: float = 7.0, amd_min_score_balanced: float = 5.0, amd_min_score_aggressive: float = 3.0) -> list[dict[str, object]]:
    if strategy == 'AMD + FVG + Supply/Demand':
        preset_config = ConfluenceConfig.for_mode(str(amd_mode))
        config = ConfluenceConfig(
            mode=str(amd_mode),
            swing_window=int(amd_swing_window),
            accumulation_lookback=preset_config.accumulation_lookback,
            manipulation_lookback=preset_config.manipulation_lookback,
            distribution_lookback=preset_config.distribution_lookback,
            min_fvg_size=float(amd_min_fvg_size),
            min_bvg_size=float(amd_min_bvg_size),
            zone_merge_tolerance=preset_config.zone_merge_tolerance,
            zone_fresh_bars=int(amd_zone_fresh_bars),
            min_zone_reaction=preset_config.min_zone_reaction,
            retest_tolerance_pct=float(amd_retest_tolerance_pct),
            max_retest_bars=int(amd_max_retest_bars),
            rr_ratio=float(rr_ratio),
            trailing_sl_pct=float(trailing_sl_pct),
            duplicate_signal_cooldown_bars=preset_config.duplicate_signal_cooldown_bars,
            min_score_conservative=float(amd_min_score_conservative),
            min_score_balanced=float(amd_min_score_balanced),
            min_score_aggressive=float(amd_min_score_aggressive),
            allow_secondary_entries=preset_config.allow_secondary_entries,
            max_trades_per_day=preset_config.max_trades_per_day,
        )
        rows = generate_amd_fvg_sd_trades(candles, capital=float(capital), risk_pct=float(risk_pct), rr_ratio=float(rr_ratio), config=config)
        rows = attach_option_strikes(rows, strike_step=int(strike_step), moneyness=str(moneyness), steps=int(strike_steps))
        return _attach_option_metrics(rows, str(symbol), bool(fetch_option_metrics))

    if strategy == 'Indicator':
        raw_rows = generate_indicator_rows(_df_to_candles(candles), config=IndicatorConfig())
        mapped: list[dict[str, object]] = []
        for row in raw_rows:
            item = dict(row)
            signal = str(item.get('market_signal', '')).upper()
            item['side'] = 'BUY' if signal in {'BULLISH_TREND', 'OVERSOLD', 'BUY', 'LONG'} else 'SELL' if signal in {'BEARISH_TREND', 'OVERBOUGHT', 'SELL', 'SHORT'} else ''
            item.setdefault('entry_price', item.get('close', item.get('price', 0.0)))
            item.setdefault('timestamp', item.get('timestamp', ''))
            item.setdefault('strategy', 'INDICATOR')
            mapped.append(item)
        rows = _attach_indicator_trade_levels(mapped, rr_ratio=float(rr_ratio), trailing_sl_pct=float(trailing_sl_pct))
        rows = attach_option_strikes(rows, strike_step=int(strike_step), moneyness=str(moneyness), steps=int(strike_steps))
        return _attach_option_metrics(rows, str(symbol), bool(fetch_option_metrics))

    context = StrategyContext(
        strategy=strategy,
        candles=candles,
        candle_rows=_df_to_candles(candles),
        capital=float(capital),
        risk_pct=float(risk_pct),
        rr_ratio=float(rr_ratio),
        trailing_sl_pct=float(trailing_sl_pct),
        symbol=str(symbol),
        strike_step=int(strike_step),
        moneyness=str(moneyness),
        strike_steps=int(strike_steps),
        fetch_option_metrics=bool(fetch_option_metrics),
        mtf_ema_period=int(mtf_ema_period),
        mtf_setup_mode=str(mtf_setup_mode),
        mtf_retest_strength=bool(mtf_retest_strength),
        mtf_max_trades_per_day=int(mtf_max_trades_per_day),
    )
    return run_strategy_workflow(
        context,
        breakout_generator=generate_breakout_trades,
        demand_supply_generator=generate_demand_supply_trades,
        indicator_generator=generate_indicator_rows,
        one_trade_generator=generate_one_trade_day_trades,
        mtf_generator=generate_mtf_trade_trades,
        attach_levels_fn=_attach_indicator_trade_levels,
        attach_option_strikes_fn=attach_option_strikes,
        attach_option_metrics_fn=_attach_option_metrics,
    )


def _metric_value(rows: list[dict[str, object]], key: str, default: float = 0.0) -> float:
    if not rows:
        return default
    try:
        return float(rows[-1].get(key, default) or default)
    except Exception:
        return default


def _save_outputs(candles: pd.DataFrame, trades: list[dict[str, object]], backtest_summary: dict[str, object]) -> None:
    write_rows(OHLCV_OUTPUT, candles.to_dict(orient='records'))
    write_rows(TRADES_OUTPUT, trades)
    write_rows(BACKTEST_OUTPUT, [backtest_summary])


def _minimal_theme() -> None:
    st.set_page_config(page_title='Trading Desk', page_icon='chart', layout='wide')
    st.markdown(
        '''
        <style>
        [data-testid="stAppViewContainer"] {
            background: linear-gradient(180deg, #061018 0%, #0a1521 100%);
        }
        .main .block-container {max-width: 900px; padding-top: 2rem;}
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
        </style>
        ''',
        unsafe_allow_html=True,
    )


def main() -> None:
    _minimal_theme()
    st.markdown('<div class="desk-card"><h2 style="margin:0;color:#e2e8f0;">Production Trading Desk</h2><p style="margin:8px 0 0 0;color:#94a3b8;">Minimal execution surface. Raw data is written to files only.</p></div>', unsafe_allow_html=True)

    strategy = st.selectbox('Strategy', ['Breakout', 'Demand Supply', 'Indicator', 'One Trade/Day', 'MTF 5m', 'AMD + FVG + Supply/Demand'])
    capital = st.number_input('Capital', min_value=1000.0, value=100000.0, step=1000.0)
    risk_pct = st.number_input('Risk %', min_value=0.1, value=1.0, step=0.1)
    rr_ratio = st.number_input('RR Ratio', min_value=1.0, value=2.0, step=0.1)
    mode = st.selectbox('Mode', ['Conservative', 'Balanced', 'Aggressive'], index=1)
    run_clicked = st.button('Run Desk', type='primary', use_container_width=True)

    if not run_clicked:
        st.info('Ready. Press Run Desk to fetch data, generate trades, backtest, and write outputs to files.')
        return

    try:
        candles = fetch_ohlcv_data(DEFAULT_SYMBOL, interval=DEFAULT_INTERVAL, period=DEFAULT_PERIOD)
        trades = run_strategy(
            strategy=strategy,
            candles=candles,
            capital=float(capital),
            risk_pct=float(risk_pct),
            rr_ratio=float(rr_ratio),
            trailing_sl_pct=0.0,
            symbol=DEFAULT_SYMBOL,
            strike_step=50,
            moneyness='ATM',
            strike_steps=0,
            fetch_option_metrics=False,
            mtf_ema_period=3,
            mtf_setup_mode='either',
            mtf_retest_strength=True,
            mtf_max_trades_per_day=3,
            amd_mode=mode,
            amd_swing_window=3,
            amd_min_fvg_size=0.35,
            amd_min_bvg_size=0.25,
            amd_zone_fresh_bars=24,
            amd_retest_tolerance_pct=0.0015,
            amd_max_retest_bars=6,
            amd_min_score_conservative=7.0,
            amd_min_score_balanced=5.0,
            amd_min_score_aggressive=3.0,
        )
        backtest_summary = run_backtest(
            candles,
            {
                'Breakout': generate_breakout_trades,
                'Demand Supply': generate_demand_supply_trades,
                'Indicator': lambda df, capital, risk_pct, rr_ratio, config=None: run_strategy(
                    strategy='Indicator',
                    candles=df,
                    capital=capital,
                    risk_pct=risk_pct * 100 if risk_pct <= 1 else risk_pct,
                    rr_ratio=rr_ratio,
                    trailing_sl_pct=0.0,
                    symbol=DEFAULT_SYMBOL,
                    strike_step=50,
                    moneyness='ATM',
                    strike_steps=0,
                    fetch_option_metrics=False,
                    mtf_ema_period=3,
                    mtf_setup_mode='either',
                    mtf_retest_strength=True,
                    mtf_max_trades_per_day=3,
                ),
                'AMD + FVG + Supply/Demand': generate_amd_fvg_sd_trades,
            }.get(strategy, generate_breakout_trades),
            BacktestConfig(
                capital=float(capital),
                risk_pct=float(risk_pct) / 100.0,
                rr_ratio=float(rr_ratio),
                trades_output=TRADES_OUTPUT,
                summary_output=BACKTEST_OUTPUT,
                strategy_name=strategy,
            ),
        )
        _save_outputs(candles, trades, backtest_summary)
        last_signal = trades[-1]['side'] if trades else 'NONE'
        cols = st.columns(4)
        cols[0].metric('Total Trades', int(backtest_summary.get('total_trades', len(trades))))
        cols[1].metric('Win Rate', f"{float(backtest_summary.get('win_rate', 0.0)):.2f}%")
        cols[2].metric('PnL', f"{float(backtest_summary.get('total_pnl', 0.0)):.2f}")
        cols[3].metric('Last Signal', str(last_signal))
        st.caption(f'Outputs written to {OHLCV_OUTPUT}, {TRADES_OUTPUT}, and {BACKTEST_OUTPUT}. Symbol={DEFAULT_SYMBOL} Interval={DEFAULT_INTERVAL} Period={DEFAULT_PERIOD}')
    except Exception as exc:
        append_log(f'Trading UI failure: {exc}')
        st.error(f'Run failed: {exc}')


if __name__ == '__main__':
    main()

