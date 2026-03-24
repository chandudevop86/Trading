from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.breakout_bot import Candle, generate_trades as generate_breakout_trades
from src.btst_bot import generate_trades as generate_btst_trades
from src.demand_supply_bot import generate_trades as generate_demand_supply_trades
from src.indicator_bot import IndicatorConfig, generate_indicator_rows, generate_trades as generate_indicator_trades
from src.mtf_trade_bot import generate_trades as generate_mtf_trade_trades
from src.one_trade_day import generate_trades as generate_one_trade_day_trades


@dataclass(slots=True)
class StrategyContext:
    strategy: str
    candles: Any
    candle_rows: list[Candle]
    capital: float
    risk_pct: float
    rr_ratio: float
    trailing_sl_pct: float
    symbol: str
    strike_step: int = 50
    moneyness: str = 'ATM'
    strike_steps: int = 0
    fetch_option_metrics: bool = False
    mtf_ema_period: int = 3
    mtf_setup_mode: str = 'either'
    mtf_retest_strength: bool = True
    mtf_max_trades_per_day: int = 3
    entry_cutoff: str = ''
    pivot_window: int = 2
    cost_bps: float = 0.0
    fixed_cost_per_trade: float = 0.0
    max_daily_loss: float | None = None
    max_trades_per_day: int | None = None


def normalize_strategy_rows(rows: list[dict[str, object]], *, strategy_name: str, symbol: str) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for idx, row in enumerate(rows, start=1):
        item = dict(row)
        item.setdefault('strategy', strategy_name.upper().replace(' ', '_'))
        item.setdefault('symbol', symbol)
        item.setdefault('trade_no', idx)
        item.setdefault('trade_label', f'Trade {idx}')
        item.setdefault('entry_time', item.get('timestamp', ''))
        if 'entry_price' not in item and 'entry' in item:
            item['entry_price'] = item.get('entry')
        if 'target_price' not in item and 'target' in item:
            item['target_price'] = item.get('target')
        normalized.append(item)
    return normalized


def enrich_actionable_rows(
    rows: list[dict[str, object]],
    *,
    symbol: str,
    strike_step: int,
    moneyness: str,
    strike_steps: int,
    fetch_option_metrics: bool,
    attach_option_strikes_fn: Callable[..., list[dict[str, object]]],
    attach_option_metrics_fn: Callable[..., list[dict[str, object]]],
) -> list[dict[str, object]]:
    actionable = [dict(r) for r in rows if str(r.get('side', '')).upper() in {'BUY', 'SELL'}]
    if not actionable:
        return rows
    actionable = attach_option_strikes_fn(actionable, strike_step=int(strike_step), moneyness=str(moneyness), steps=int(strike_steps))
    actionable = attach_option_metrics_fn(actionable, symbol=str(symbol), fetch_option_metrics=bool(fetch_option_metrics))
    keyed_actionable = {
        f"{row.get('trade_no', '')}|{row.get('entry_time', row.get('timestamp', ''))}|{row.get('side', '')}": row
        for row in actionable
    }
    merged: list[dict[str, object]] = []
    for row in rows:
        key = f"{row.get('trade_no', '')}|{row.get('entry_time', row.get('timestamp', ''))}|{row.get('side', '')}"
        enriched = keyed_actionable.get(key)
        if enriched is not None:
            updated = dict(row)
            updated.update(enriched)
            merged.append(updated)
        else:
            merged.append(row)
    return merged


def generate_strategy_rows(
    context: StrategyContext,
    *,
    breakout_generator: Callable[..., list[dict[str, object]]] = generate_breakout_trades,
    demand_supply_generator: Callable[..., list[dict[str, object]]] = generate_demand_supply_trades,
    indicator_trade_generator: Callable[..., list[dict[str, object]]] = generate_indicator_trades,
    indicator_row_generator: Callable[..., list[dict[str, object]]] = generate_indicator_rows,
    one_trade_generator: Callable[..., list[dict[str, object]]] = generate_one_trade_day_trades,
    mtf_generator: Callable[..., list[dict[str, object]]] = generate_mtf_trade_trades,
    btst_generator: Callable[..., list[dict[str, object]]] = generate_btst_trades,
) -> list[dict[str, object]]:
    strategy_name = str(context.strategy or 'Breakout').strip()
    risk_fraction = float(context.risk_pct) / 100.0
    if strategy_name == 'Breakout':
        rows = breakout_generator(context.candles, capital=float(context.capital), risk_pct=risk_fraction, rr_ratio=float(context.rr_ratio), trailing_sl_pct=float(context.trailing_sl_pct))
    elif strategy_name == 'Demand Supply':
        rows = demand_supply_generator(context.candles, capital=float(context.capital), risk_pct=risk_fraction, rr_ratio=float(context.rr_ratio))
    elif strategy_name == 'Indicator':
        try:
            rows = indicator_trade_generator(context.candles, capital=float(context.capital), risk_pct=risk_fraction, rr_ratio=float(context.rr_ratio), config=IndicatorConfig())
        except Exception:
            raw_rows = indicator_row_generator(context.candle_rows, config=IndicatorConfig())
            rows = []
            for row in raw_rows:
                side = 'BUY' if str(row.get('market_signal', '')).upper() in {'BULLISH_TREND', 'OVERSOLD', 'BUY', 'LONG'} else 'SELL' if str(row.get('market_signal', '')).upper() in {'BEARISH_TREND', 'OVERBOUGHT', 'SELL', 'SHORT'} else ''
                if side:
                    item = dict(row)
                    item['side'] = side
                    item['entry'] = item.get('close', item.get('price', 0.0))
                    item['entry_price'] = item.get('entry')
                    item['target'] = item.get('entry')
                    item['target_price'] = item.get('entry')
                    item['stop_loss'] = item.get('entry')
                    item['score'] = 0.0
                    item['reason'] = str(item.get('market_signal', 'SIGNAL'))
                    rows.append(item)
    elif strategy_name == 'One Trade/Day':
        rows = one_trade_generator(context.candle_rows, capital=float(context.capital), risk_pct=risk_fraction, rr_ratio=float(context.rr_ratio), config=IndicatorConfig(), trailing_sl_pct=float(context.trailing_sl_pct))
    elif strategy_name == 'MTF 5m':
        rows = mtf_generator(context.candle_rows, capital=float(context.capital), risk_pct=risk_fraction, rr_ratio=float(context.rr_ratio), trailing_sl_pct=float(context.trailing_sl_pct), ema_period=int(context.mtf_ema_period), setup_mode=str(context.mtf_setup_mode), require_retest_strength=bool(context.mtf_retest_strength), max_trades_per_day=int(context.mtf_max_trades_per_day))
    elif strategy_name == 'BTST':
        rows = btst_generator(context.candle_rows, capital=float(context.capital), risk_pct=risk_fraction, allow_stbt=True, cost_bps=float(context.cost_bps), fixed_cost_per_trade=float(context.fixed_cost_per_trade), max_daily_loss=context.max_daily_loss, max_trades_per_day=context.max_trades_per_day)
    else:
        rows = breakout_generator(context.candles, capital=float(context.capital), risk_pct=risk_fraction, rr_ratio=float(context.rr_ratio), trailing_sl_pct=float(context.trailing_sl_pct))
    return normalize_strategy_rows(rows, strategy_name=strategy_name, symbol=context.symbol)


def run_strategy_workflow(
    context: StrategyContext,
    *,
    breakout_generator: Callable[..., list[dict[str, object]]] = generate_breakout_trades,
    demand_supply_generator: Callable[..., list[dict[str, object]]] = generate_demand_supply_trades,
    indicator_trade_generator: Callable[..., list[dict[str, object]]] = generate_indicator_trades,
    indicator_generator: Callable[..., list[dict[str, object]]] = generate_indicator_rows,
    one_trade_generator: Callable[..., list[dict[str, object]]] = generate_one_trade_day_trades,
    mtf_generator: Callable[..., list[dict[str, object]]] = generate_mtf_trade_trades,
    btst_generator: Callable[..., list[dict[str, object]]] = generate_btst_trades,
    attach_option_strikes_fn: Callable[..., list[dict[str, object]]] | None = None,
    attach_option_metrics_fn: Callable[..., list[dict[str, object]]] | None = None,
    attach_levels_fn: Callable[..., list[dict[str, object]]] | None = None,
) -> list[dict[str, object]]:
    rows = generate_strategy_rows(
        context,
        breakout_generator=breakout_generator,
        demand_supply_generator=demand_supply_generator,
        indicator_trade_generator=indicator_trade_generator,
        indicator_row_generator=indicator_generator,
        one_trade_generator=one_trade_generator,
        mtf_generator=mtf_generator,
        btst_generator=btst_generator,
    )
    if attach_option_strikes_fn is None or attach_option_metrics_fn is None:
        return rows
    return enrich_actionable_rows(
        rows,
        symbol=context.symbol,
        strike_step=context.strike_step,
        moneyness=context.moneyness,
        strike_steps=context.strike_steps,
        fetch_option_metrics=context.fetch_option_metrics,
        attach_option_strikes_fn=attach_option_strikes_fn,
        attach_option_metrics_fn=attach_option_metrics_fn,
    )
