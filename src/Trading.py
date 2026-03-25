from __future__ import annotations

import math
import os
import sys
from datetime import datetime
from pathlib import Path
import shutil

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from typing import Any, Callable

import pandas as pd
import streamlit as st

from src.amd_fvg_sd_bot import ConfluenceConfig, generate_trades as generate_amd_fvg_sd_trades
from src.backtest_engine import run_backtest, summarize_trade_log
from src.strategy_tuning import normalize_strategy_key, strategy_backtest_config
from src.execution_engine import build_execution_candidates, close_paper_trades, execute_live_trades, execute_paper_trades, execution_result_summary
from src.breakout_bot import Candle, generate_trades as generate_breakout_trades
from src.demand_supply_bot import generate_trades as generate_demand_supply_trades
from src.indicator_bot import IndicatorConfig, generate_indicator_rows
from src.live_ohlcv import fetch_live_ohlcv
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.strike_selector import attach_option_strikes
from src.strategy_service import StrategyContext, run_strategy_workflow
from src.trading_core import append_log, configure_file_logging, prepare_trading_data, write_rows

DATA_DIR = Path('data')
LOG_DIR = Path('logs')
OHLCV_OUTPUT = DATA_DIR / 'ohlcv.csv'
LIVE_OHLCV_OUTPUT = DATA_DIR / 'live_ohlcv.csv'
TRADES_OUTPUT = DATA_DIR / 'trades.csv'
SIGNAL_OUTPUT = DATA_DIR / 'output.csv'
EXECUTED_TRADES_OUTPUT = DATA_DIR / 'executed_trades.csv'
PAPER_LOG_OUTPUT = DATA_DIR / 'paper_trading_logs_all.csv'
LIVE_LOG_OUTPUT = DATA_DIR / 'live_trading_logs_all.csv'
PAPER_SUMMARY_OUTPUT = DATA_DIR / 'paper_trade_summary.csv'
BACKTEST_TRADES_OUTPUT = DATA_DIR / 'backtest_trades.csv'
BACKTEST_SUMMARY_OUTPUT = DATA_DIR / 'backtest_summary.csv'
BACKTEST_RESULTS_OUTPUT = DATA_DIR / 'backtest_results_all.csv'
BACKTEST_VALIDATION_UI_OUTPUT = DATA_DIR / 'backtest_validation.csv'
STRATEGY_RANKING_OUTPUT = DATA_DIR / 'strategy_expectancy_report.csv'
OPTIMIZER_OUTPUT = DATA_DIR / 'strategy_optimizer_report.csv'
ORDER_HISTORY_OUTPUT = DATA_DIR / 'order_history.csv'
APP_LOG = LOG_DIR / 'app.log'
BROKER_LOG = LOG_DIR / 'broker.log'
EXECUTION_LOG = LOG_DIR / 'execution.log'
REJECTIONS_LOG = LOG_DIR / 'rejections.log'
ERRORS_LOG = LOG_DIR / 'errors.log'
DEFAULT_SYMBOL = os.getenv('TRADING_SYMBOL', '^NSEI').strip() or '^NSEI'
DEFAULT_INTERVAL = os.getenv('TRADING_INTERVAL', '5m').strip() or '5m'
DEFAULT_PERIOD = os.getenv('TRADING_PERIOD', '5d').strip() or '5d'
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
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            pd.DataFrame().to_csv(path, index=False)
    for path in [APP_LOG, BROKER_LOG, EXECUTION_LOG, REJECTIONS_LOG, ERRORS_LOG]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)


def _period_for_interval(interval: str) -> str:
    mapping = {
        '1m': '7d',
        '5m': '60d',
        '15m': '60d',
        '30m': '60d',
        '1h': '730d',
        '1d': '1y',
    }
    return mapping.get(interval, DEFAULT_PERIOD)


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
    return _safe_float(rows[-1].get(key, default), default)


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


def _strategy_callable(strategy: str, symbol: str) -> Callable[[pd.DataFrame, float, float, float, Any], list[dict[str, object]]]:
    mapping: dict[str, Callable[[pd.DataFrame, float, float, float, Any], list[dict[str, object]]]] = {
        'Breakout': generate_breakout_trades,
        'Demand Supply': generate_demand_supply_trades,
        'AMD + FVG + Supply/Demand': generate_amd_fvg_sd_trades,
        'Indicator': lambda df, capital, risk_pct, rr_ratio, config=None: run_strategy(
            strategy='Indicator',
            candles=df,
            capital=capital,
            risk_pct=risk_pct * 100 if risk_pct <= 1 else risk_pct,
            rr_ratio=rr_ratio,
            trailing_sl_pct=0.0,
            symbol=symbol,
            strike_step=50,
            moneyness='ATM',
            strike_steps=0,
            fetch_option_metrics=False,
            mtf_ema_period=3,
            mtf_setup_mode='either',
            mtf_retest_strength=True,
            mtf_max_trades_per_day=3,
        ),
    }
    return mapping.get(strategy, generate_breakout_trades)


def _save_runtime_outputs(candles: pd.DataFrame, trades: list[dict[str, object]]) -> None:
    candle_rows = candles.to_dict(orient='records')
    write_rows(OHLCV_OUTPUT, candle_rows)
    write_rows(LIVE_OHLCV_OUTPUT, candle_rows)
    write_rows(TRADES_OUTPUT, trades)
    write_rows(SIGNAL_OUTPUT, trades)
    if not EXECUTED_TRADES_OUTPUT.exists() or EXECUTED_TRADES_OUTPUT.stat().st_size == 0:
        pd.DataFrame().to_csv(EXECUTED_TRADES_OUTPUT, index=False)
    if not PAPER_LOG_OUTPUT.exists() or PAPER_LOG_OUTPUT.stat().st_size == 0:
        pd.DataFrame().to_csv(PAPER_LOG_OUTPUT, index=False)
    if not LIVE_LOG_OUTPUT.exists() or LIVE_LOG_OUTPUT.stat().st_size == 0:
        pd.DataFrame().to_csv(LIVE_LOG_OUTPUT, index=False)


def _mirror_output_file(source: Path, *destinations: Path) -> None:
    if not source.exists():
        return
    for destination in destinations:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)


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


def _load_best_strategy_profile(backtest_summary: dict[str, object], strategy: str) -> str:
    ranking_rows: list[dict[str, object]] = []
    if STRATEGY_RANKING_OUTPUT.exists() and STRATEGY_RANKING_OUTPUT.stat().st_size > 0:
        try:
            ranking_frame = pd.read_csv(STRATEGY_RANKING_OUTPUT)
            ranking_rows = ranking_frame.to_dict(orient='records')
        except Exception:
            ranking_rows = []

    if not ranking_rows and backtest_summary:
        candidate = dict(backtest_summary)
        candidate.setdefault('strategy', strategy)
        ranking_rows = rank_strategy_summaries([candidate])

    if not ranking_rows:
        return 'No ranked strategy profile available yet.'

    best = dict(ranking_rows[0])
    rank = int(_safe_float(best.get('rank', 1), 1.0))
    strategy_name = str(best.get('strategy', strategy) or strategy)
    expectancy = _safe_float(best.get('expectancy_per_trade'))
    profit_factor = str(best.get('profit_factor', '0.0'))
    positive_expectancy = str(best.get('positive_expectancy', 'NO')).upper()
    max_drawdown = _safe_float(best.get('max_drawdown'))
    win_rate = _safe_float(best.get('win_rate'))
    total_trades = int(_safe_float(best.get('total_trades', best.get('trades', 0))))
    return (
        f'Rank {rank} | {strategy_name} | Expectancy {expectancy:.2f} | '
        f'PF {profit_factor} | Win Rate {win_rate:.2f}% | '
        f'Max DD {max_drawdown:.2f} | Trades {total_trades} | '
        f'Edge {positive_expectancy}'
    )



def _latest_optimizer_gate(strategy: str) -> tuple[bool, str]:
    if not OPTIMIZER_OUTPUT.exists() or OPTIMIZER_OUTPUT.stat().st_size == 0:
        return False, 'optimizer report missing'
    try:
        frame = pd.read_csv(OPTIMIZER_OUTPUT)
    except Exception:
        return False, 'optimizer report unreadable'
    if frame.empty:
        return False, 'optimizer report empty'
    strategy_key = normalize_strategy_key(strategy)
    frame['normalized_strategy'] = frame['strategy'].map(lambda value: normalize_strategy_key(str(value)))
    matched = frame[frame["normalized_strategy"] == strategy_key].copy()
    if matched.empty:
        return False, f'no optimizer row for {strategy}'
    if 'optimizer_rank' in matched.columns:
        matched = matched.sort_values('optimizer_rank')
    elif 'rank_score' in matched.columns:
        matched = matched.sort_values('rank_score', ascending=False)
    row = matched.iloc[0].to_dict()
    deployment_ready = str(row.get('deployment_ready', 'NO')).strip().upper() == 'YES'
    blockers = str(row.get('deployment_blockers', '') or '').strip()
    if deployment_ready:
        return True, 'optimizer validated'
    return False, blockers or 'optimizer gate failed'

def _status_message(run_clicked: bool, backtest_clicked: bool) -> str:
    if backtest_clicked:
        return 'Backtest completed'
    if run_clicked:
        return 'Run completed'
    return 'Ready'


def _load_csv_summary(path: Path) -> dict[str, object]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        frame = pd.read_csv(path)
    except Exception:
        return {}
    if frame.empty:
        return {}
    return frame.iloc[-1].to_dict()


def _load_csv_rows(path: Path) -> list[dict[str, object]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        frame = pd.read_csv(path)
    except Exception:
        return []
    if frame.empty:
        return []
    return frame.to_dict(orient='records')


def _current_execution_rows(path: Path, strategy: str, symbol: str, execution_type: str = 'PAPER') -> list[dict[str, object]]:
    normalized_strategy = normalize_strategy_key(strategy)
    normalized_symbol = (symbol or '').strip().upper()
    rows = _load_csv_rows(path)
    filtered: list[dict[str, object]] = []
    for row in rows:
        if str(row.get('execution_type', '') or '').strip().upper() != execution_type:
            continue
        row_strategy = normalize_strategy_key(str(row.get('strategy', '') or ''))
        row_symbol = str(row.get('symbol', '') or '').strip().upper()
        if normalized_strategy and row_strategy and row_strategy != normalized_strategy:
            continue
        if normalized_symbol and row_symbol and row_symbol != normalized_symbol:
            continue
        filtered.append(dict(row))
    if filtered or not normalized_symbol:
        return filtered
    return [
        dict(row)
        for row in rows
        if str(row.get('execution_type', '') or '').strip().upper() == execution_type
        and normalize_strategy_key(str(row.get('strategy', '') or '')) == normalized_strategy
    ]


def _paper_execution_summary(path: Path, strategy: str, symbol: str, capital: float) -> dict[str, object]:
    rows = _current_execution_rows(path, strategy, symbol, execution_type='PAPER')
    if not rows:
        return {}
    return summarize_trade_log(rows, capital=float(capital), strategy_name=normalize_strategy_key(strategy) or 'PAPER_EXECUTION')


def _todays_trade_count(path: Path, strategy: str, symbol: str, execution_type: str = 'PAPER') -> int:
    rows = _current_execution_rows(path, strategy, symbol, execution_type=execution_type)
    if not rows:
        return 0
    today_key = datetime.now().strftime('%Y-%m-%d')
    count = 0
    for row in rows:
        status = str(row.get('execution_status', '') or '').strip().upper()
        if status not in {'EXECUTED', 'FILLED', 'CLOSED', 'EXITED'}:
            continue
        for column in ('executed_at_utc', 'exit_time', 'entry_time', 'signal_time', 'timestamp'):
            value = str(row.get(column, '') or '').strip()
            if value[:10] == today_key:
                count += 1
                break
    return count


def _active_summary(backtest_summary: dict[str, object], paper_summary: dict[str, object]) -> dict[str, object]:
    return dict(backtest_summary or paper_summary or {})


def _short_broker_status(broker_choice: str, broker_status: str) -> str:
    if broker_choice == 'Dhan Live':
        return 'Dhan live active' if 'armed' in broker_status.lower() else broker_status
    return 'Paper broker active'


def _store_rejection(message: str) -> None:
    _append_text_log(REJECTIONS_LOG, message)
    append_log(message)


def _run_live_strategy(strategy: str, symbol: str, timeframe: str, capital: float, risk_pct: float, rr_ratio: float, mode: str) -> tuple[pd.DataFrame, list[dict[str, object]], str]:
    period = _period_for_interval(timeframe)
    candles = fetch_ohlcv_data(symbol, interval=timeframe, period=period)
    trades = run_strategy(
        strategy=strategy,
        candles=candles,
        capital=float(capital),
        risk_pct=float(risk_pct),
        rr_ratio=float(rr_ratio),
        trailing_sl_pct=0.0,
        symbol=symbol,
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
    normalized_trades: list[dict[str, object]] = []
    for trade in trades:
        item = dict(trade)
        item.setdefault('symbol', symbol)
        item.setdefault('timeframe', timeframe)
        item.setdefault('duplicate_signal_cooldown_bars', item.get('duplicate_signal_cooldown_bars', 0))
        normalized_trades.append(item)
    trades = normalized_trades
    _save_runtime_outputs(candles, trades)
    if trades:
        _append_text_log(EXECUTION_LOG, f'RUN {strategy} {symbol} {timeframe} trades={len(trades)}')
    else:
        _store_rejection(f'RUN {strategy} {symbol} {timeframe} produced no trades.')
    return candles, trades, period


def _run_strategy_backtest(candles: pd.DataFrame, strategy: str, symbol: str, capital: float, risk_pct: float, rr_ratio: float) -> dict[str, object]:
    config = strategy_backtest_config(
        strategy,
        capital=float(capital),
        risk_pct=float(risk_pct) / 100.0,
        rr_ratio=float(rr_ratio),
        trades_output=BACKTEST_TRADES_OUTPUT,
        summary_output=BACKTEST_SUMMARY_OUTPUT,
        validation_output=BACKTEST_VALIDATION_UI_OUTPUT,
    )
    summary = run_backtest(candles, _strategy_callable(strategy, symbol), config)
    _mirror_output_file(BACKTEST_SUMMARY_OUTPUT, BACKTEST_RESULTS_OUTPUT)
    return summary


def _paper_candle_rows(candles: pd.DataFrame) -> list[dict[str, object]]:
    prepared = prepare_trading_data(candles)
    return prepared.to_dict(orient='records')


def _refresh_paper_trade_summary(candles: pd.DataFrame, capital: float) -> tuple[list[dict[str, object]], dict[str, object]]:
    closed_rows = close_paper_trades(EXECUTED_TRADES_OUTPUT, _paper_candle_rows(candles), max_hold_minutes=60)
    summary = summarize_trade_log(
        EXECUTED_TRADES_OUTPUT,
        capital=float(capital),
        strategy_name='PAPER_EXECUTION',
        summary_output=PAPER_SUMMARY_OUTPUT,
        validation_output=BACKTEST_VALIDATION_UI_OUTPUT,
    )
    return closed_rows, summary

def _run_execution(strategy: str, trades: list[dict[str, object]], symbol: str, broker_choice: str, candles: pd.DataFrame, capital: float) -> tuple[object | None, list[tuple[str, str]], str]:
    if not trades:
        return None, [('info', 'No actionable trade candidates')], 'Paper standby'
    candidates = build_execution_candidates(strategy, trades, symbol)
    if broker_choice == 'Dhan Live':
        optimizer_ready, optimizer_reason = _latest_optimizer_gate(strategy)
        if not optimizer_ready:
            _store_rejection(f'LIVE BLOCKED {strategy} {symbol}: {optimizer_reason}')
            return None, [('warning', f'Live blocked: {optimizer_reason}')], 'Live broker blocked by optimizer gate'
        live_enabled = str(os.getenv('LIVE_TRADING_ENABLED', '') or '').strip().lower() in {'1', 'true', 'yes', 'on'}
        result = execute_live_trades(
            candidates,
            EXECUTED_TRADES_OUTPUT,
            broker_name='DHAN',
            live_enabled=live_enabled,
            max_trades_per_day=1,
            max_open_trades=1,
            order_history_path=ORDER_HISTORY_OUTPUT,
        )
        _mirror_output_file(EXECUTED_TRADES_OUTPUT, LIVE_LOG_OUTPUT)
        status = 'Live broker armed' if live_enabled else 'Live broker blocked by config'
    else:
        result = execute_paper_trades(
            candidates,
            EXECUTED_TRADES_OUTPUT,
            max_trades_per_day=1,
            max_open_trades=1,
            order_history_path=ORDER_HISTORY_OUTPUT,
        )
        _mirror_output_file(EXECUTED_TRADES_OUTPUT, PAPER_LOG_OUTPUT)
        _refresh_paper_trade_summary(candles, capital)
        status = 'Paper broker active'
    return result, execution_result_summary(result), status

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
    row_one[1].metric('Win Rate', f'{win_rate:.2f}%' )
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


def main() -> None:
    _ensure_output_files()
    _minimal_theme()
    st.markdown(
        '<div class="desk-card"><h2 style="margin:0;color:#e2e8f0;">Production Trading Desk</h2><p style="margin:8px 0 0 0;color:#94a3b8;">Minimal operator controls with live execution and historical validation routed to files.</p></div>',
        unsafe_allow_html=True,
    )

    control_col_1, control_col_2, control_col_3 = st.columns(3)
    with control_col_1:
        symbol = st.text_input('Symbol', value=DEFAULT_SYMBOL)
        strategy = st.selectbox('Strategy', STRATEGY_OPTIONS)
        broker_choice = st.selectbox('Broker', BROKER_OPTIONS)
    with control_col_2:
        timeframe = st.selectbox('Timeframe', TIMEFRAME_OPTIONS, index=TIMEFRAME_OPTIONS.index(DEFAULT_INTERVAL) if DEFAULT_INTERVAL in TIMEFRAME_OPTIONS else 1)
        capital = st.number_input('Capital', min_value=1000.0, value=100000.0, step=1000.0)
        risk_pct = st.number_input('Risk %', min_value=0.1, value=1.0, step=0.1)
    with control_col_3:
        rr_ratio = st.number_input('RR Ratio', min_value=1.0, value=2.0, step=0.1)
        mode = st.selectbox('Mode', ['Conservative', 'Balanced', 'Aggressive'], index=1)
        period = _period_for_interval(timeframe)
        st.caption(f'Fetch window: {period}')
        action_row = st.columns(2)
        st.markdown('<div class="desk-label">Run</div>', unsafe_allow_html=True)
        run_clicked = action_row[0].button('Run', type='primary', use_container_width=True)
        st.markdown('<div class="desk-label">Backtest</div>', unsafe_allow_html=True)
        backtest_clicked = action_row[1].button('Backtest', use_container_width=True)

    if not run_clicked and not backtest_clicked:
        _render_summary_cards([], {}, 0)
        _render_operator_panels('Ready', [], symbol.strip() or DEFAULT_SYMBOL, timeframe, _period_for_interval(timeframe), broker_choice, 'Paper broker active')
        return
    try:
        normalized_symbol = symbol.strip() or DEFAULT_SYMBOL
        candles, trades, period = _run_live_strategy(strategy, normalized_symbol, timeframe, float(capital), float(risk_pct), float(rr_ratio), mode)
        broker_status = 'Paper broker active' if broker_choice == 'Paper' else 'Idle'
        if run_clicked:
            _, _, broker_status = _run_execution(strategy, trades, normalized_symbol, broker_choice, candles, capital)
            _append_text_log(APP_LOG, f'EXECUTION completed for {strategy} {normalized_symbol} broker={broker_choice}')
        backtest_summary = st.session_state.get('backtest_summary', {})

        if backtest_clicked:
            backtest_summary = _run_strategy_backtest(candles, strategy, normalized_symbol, float(capital), float(risk_pct), float(rr_ratio))
            st.session_state['backtest_summary'] = backtest_summary
            _append_text_log(APP_LOG, f'BACKTEST completed for {strategy} {normalized_symbol} {timeframe}')
        elif not backtest_summary:
            backtest_summary = {}

        paper_summary = _paper_execution_summary(EXECUTED_TRADES_OUTPUT, strategy, normalized_symbol, float(capital))
        active_summary = _active_summary(backtest_summary if backtest_clicked else {}, paper_summary)
        todays_trades = _todays_trade_count(EXECUTED_TRADES_OUTPUT, strategy, normalized_symbol)
        status = _status_message(run_clicked, backtest_clicked)
        _append_text_log(APP_LOG, status)
        _render_summary_cards(trades, active_summary, todays_trades)
        _render_operator_panels(status, trades, normalized_symbol, timeframe, period, broker_choice, broker_status)
    except Exception as exc:
        message = f'Trading UI failure: {exc}'
        _append_text_log(APP_LOG, message)
        _append_text_log(ERRORS_LOG, message)
        append_log(message)
        st.error(f'Run failed: {exc}')


if __name__ == '__main__':
    main()

