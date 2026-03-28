from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal

from src.amd_fvg_sd_bot import ConfluenceConfig, generate_trades as generate_amd_fvg_sd_trades
from src.breakout_bot import BreakoutConfig, Candle, generate_trades as generate_breakout_trades
from src.btst_bot import BtstConfig, generate_trades as generate_btst_trades
from src.strategy_demand_supply import DemandSupplyConfig, generate_trades as generate_demand_supply_trades
from src.indicator_bot import IndicatorConfig, generate_indicator_rows, generate_trades as generate_indicator_trades
from src.mtf_trade_bot import MtfTradeConfig, generate_trades as generate_mtf_trade_trades
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.strategy_tuning import strategy_tuning_preset

STRATEGY_SIGNAL_CONTRACT_VERSION = 'legacy_strategy_signal_v1'
_ACTIONABLE_SIDES = {'BUY', 'SELL'}


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
    mode: str = 'Balanced'
    amd_mode: str = 'Balanced'
    amd_swing_window: int = 3
    amd_min_fvg_size: float = 0.35
    amd_min_bvg_size: float = 0.25
    amd_zone_fresh_bars: int = 24
    amd_retest_tolerance_pct: float = 0.0015
    amd_max_retest_bars: int = 6
    amd_min_score_conservative: float = 7.0
    amd_min_score_balanced: float = 5.0
    amd_min_score_aggressive: float = 3.0


@dataclass(slots=True)
class StrategyDependencies:
    breakout_generator: Callable[..., list[dict[str, object]]] = generate_breakout_trades
    demand_supply_generator: Callable[..., list[dict[str, object]]] = generate_demand_supply_trades
    indicator_trade_generator: Callable[..., list[dict[str, object]]] = generate_indicator_trades
    indicator_row_generator: Callable[..., list[dict[str, object]]] = generate_indicator_rows
    one_trade_generator: Callable[..., list[dict[str, object]]] = generate_one_trade_day_trades
    mtf_generator: Callable[..., list[dict[str, object]]] = generate_mtf_trade_trades
    btst_generator: Callable[..., list[dict[str, object]]] = generate_btst_trades
    amd_generator: Callable[..., list[dict[str, object]]] = generate_amd_fvg_sd_trades


@dataclass(frozen=True, slots=True)
class StrategyDefinition:
    name: str
    input_mode: Literal['candles', 'candle_rows']
    runner: Callable[[StrategyContext, StrategyDependencies], list[dict[str, object]]]


def _safe_float(value: object) -> float | None:
    try:
        if value is None or str(value).strip() == '':
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_trade_price(row: dict[str, object], *keys: str) -> object:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        numeric = _safe_float(value)
        if numeric is not None:
            return round(numeric, 4)
        if value not in (None, ''):
            return value
    return ''


def _coerce_trade_quantity(row: dict[str, object]) -> int:
    quantity = _safe_float(row.get('quantity', 0))
    return int(quantity) if quantity is not None else 0


def _canonical_strategy_name(strategy_name: str) -> str:
    normalized = str(strategy_name or 'Breakout').strip()
    if normalized == 'Demand Supply (Retest)':
        return 'DEMAND_SUPPLY'
    return normalized.upper().replace(' ', '_')

def _base_contract_fields(row: dict[str, object], *, strategy_name: str, symbol: str, trade_no: int) -> dict[str, object]:
    timestamp = str(row.get('timestamp') or row.get('entry_time') or row.get('signal_time') or row.get('time') or row.get('date') or '')
    side = str(row.get('side') or row.get('type') or '').upper()
    base = dict(row)
    base['timestamp'] = timestamp
    base['entry_time'] = str(base.get('entry_time') or timestamp)
    base['signal_time'] = str(base.get('signal_time') or base['entry_time'])
    base['side'] = side if side in _ACTIONABLE_SIDES else str(base.get('side') or base.get('type') or '')
    base['strategy'] = str(base.get('strategy') or _canonical_strategy_name(strategy_name))
    base['symbol'] = str(base.get('symbol') or symbol)
    base['trade_no'] = int(base.get('trade_no') or trade_no)
    base['trade_label'] = str(base.get('trade_label') or f'Trade {trade_no}')
    base['contract_version'] = STRATEGY_SIGNAL_CONTRACT_VERSION
    base['source_strategy'] = str(strategy_name)
    return base
def standardize_strategy_rows(rows: list[dict[str, object]], *, strategy_name: str, symbol: str) -> list[dict[str, object]]:
    standardized: list[dict[str, object]] = []
    for idx, row in enumerate(rows, start=1):
        item = _base_contract_fields(dict(row), strategy_name=strategy_name, symbol=symbol, trade_no=idx)
        side = str(item.get('side', '')).upper()
        item['side'] = side if side in _ACTIONABLE_SIDES else str(item.get('side', ''))
        item['reason'] = str(item.get('reason', item.get('market_signal', '')) or '')
        score = _safe_float(item.get('score', item.get('total_score', 0.0)))
        item['score'] = round(score, 2) if score is not None else 0.0
        item['quantity'] = _coerce_trade_quantity(item)

        entry_value = _coerce_trade_price(item, 'entry', 'entry_price', 'price', 'close')
        stop_loss = _coerce_trade_price(item, 'stop_loss', 'sl', 'trailing_stop_loss', 'entry_price', 'entry', 'price', 'close')
        target_value = _coerce_trade_price(item, 'target', 'target_price', 'tp')

        item['entry'] = entry_value
        item['entry_price'] = _coerce_trade_price(item, 'entry_price', 'entry', 'price', 'close')
        item['stop_loss'] = stop_loss
        item['target'] = target_value
        item['target_price'] = _coerce_trade_price(item, 'target_price', 'target', 'tp')
        item['price'] = _coerce_trade_price(item, 'price', 'entry', 'entry_price', 'close')

        entry_numeric = _safe_float(item.get('entry'))
        stop_numeric = _safe_float(item.get('stop_loss'))
        risk_per_unit = abs(entry_numeric - stop_numeric) if entry_numeric is not None and stop_numeric is not None else 0.0
        item['risk_per_unit'] = round(risk_per_unit, 4)
        standardized.append(item)
    return standardized
def normalize_strategy_rows(rows: list[dict[str, object]], *, strategy_name: str, symbol: str) -> list[dict[str, object]]:
    return standardize_strategy_rows(rows, strategy_name=strategy_name, symbol=symbol)


def contextless_strategy_name(rows: list[dict[str, object]], default: str) -> str:
    for row in rows:
        value = str(row.get('source_strategy') or row.get('strategy') or '').strip()
        if value:
            return value
    return default


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
    actionable = [dict(r) for r in rows if str(r.get('side', '')).upper() in _ACTIONABLE_SIDES]
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
    return standardize_strategy_rows(merged, strategy_name=contextless_strategy_name(rows, ''), symbol=symbol)


def _run_breakout_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    preset = strategy_tuning_preset('BREAKOUT')
    configured_max_trades = context.max_trades_per_day if context.max_trades_per_day is not None else preset.max_trades_per_day
    return dependencies.breakout_generator(
        context.candles,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
        config=BreakoutConfig(
            trailing_sl_pct=float(context.trailing_sl_pct),
            cost_bps=float(context.cost_bps),
            fixed_cost_per_trade=float(context.fixed_cost_per_trade),
            max_daily_loss=context.max_daily_loss,
            max_trades_per_day=max(1, int(configured_max_trades or 1)),
            duplicate_signal_cooldown_bars=max(12, int(preset.duplicate_signal_cooldown_bars)),
            min_breakout_strength=0.22,
            min_volume_ratio=1.30,
            require_vwap_alignment=True,
            require_market_structure=True,
            allow_secondary_entries=False,
            allow_afternoon_session=False,
        ),
    )


def _run_demand_supply_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    preset = strategy_tuning_preset('DEMAND_SUPPLY')
    configured_max_trades = context.max_trades_per_day if context.max_trades_per_day is not None else preset.max_trades_per_day
    configured_mode = str(context.mode or 'Balanced').strip() or 'Balanced'
    return dependencies.demand_supply_generator(
        context.candles,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
        config=DemandSupplyConfig(
            mode=configured_mode,
            trailing_sl_pct=float(context.trailing_sl_pct),
            pivot_window=max(1, int(context.pivot_window)),
            max_trades_per_day=max(1, int(configured_max_trades or 1)),
            duplicate_signal_cooldown_bars=max(24, int(preset.duplicate_signal_cooldown_bars)),
            require_vwap_alignment=True,
            require_trend_bias=True,
            require_market_structure=True,
            max_retest_bars=4,
            min_reaction_strength=0.75,
            min_zone_selection_score=5.0,
            min_confirmation_body_ratio=0.60,
            min_rejection_wick_ratio=0.50,
            zone_departure_buffer_pct=0.0006,
            vwap_reclaim_buffer_pct=0.0005,
            allow_afternoon_session=False,
            avoid_midday=True,
        ),
    )


def _run_indicator_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    raw_rows = dependencies.indicator_row_generator(context.candle_rows, config=IndicatorConfig())
    rows: list[dict[str, object]] = []
    for row in raw_rows:
        signal = str(row.get('market_signal', '')).upper()
        side = 'BUY' if signal in {'BULLISH_TREND', 'OVERSOLD', 'BUY', 'LONG'} else 'SELL' if signal in {'BEARISH_TREND', 'OVERBOUGHT', 'SELL', 'SHORT'} else ''
        if not side:
            continue
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
    return rows


def _run_one_trade_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    return dependencies.one_trade_generator(
        context.candle_rows,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
        config=IndicatorConfig(),
        trailing_sl_pct=float(context.trailing_sl_pct),
    )


def _run_mtf_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    return dependencies.mtf_generator(
        context.candle_rows,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
        config=MtfTradeConfig(
            trailing_sl_pct=float(context.trailing_sl_pct),
            ema_period=int(context.mtf_ema_period),
            setup_mode=str(context.mtf_setup_mode),
            require_retest_strength=bool(context.mtf_retest_strength),
            max_trades_per_day=1,
            cost_bps=float(context.cost_bps),
            fixed_cost_per_trade=float(context.fixed_cost_per_trade),
            max_daily_loss=context.max_daily_loss,
        ),
    )
def _run_btst_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    return dependencies.btst_generator(
        context.candle_rows,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
        config=BtstConfig(
            allow_stbt=True,
            cost_bps=float(context.cost_bps),
            fixed_cost_per_trade=float(context.fixed_cost_per_trade),
            max_daily_loss=context.max_daily_loss,
            max_trades_per_day=context.max_trades_per_day,
        ),
    )
def _run_amd_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    preset_config = ConfluenceConfig.for_mode(str(context.amd_mode))
    config = ConfluenceConfig(
        mode=str(context.amd_mode),
        swing_window=int(context.amd_swing_window),
        accumulation_lookback=preset_config.accumulation_lookback,
        manipulation_lookback=preset_config.manipulation_lookback,
        distribution_lookback=preset_config.distribution_lookback,
        min_fvg_size=float(context.amd_min_fvg_size),
        min_bvg_size=float(context.amd_min_bvg_size),
        zone_merge_tolerance=preset_config.zone_merge_tolerance,
        zone_fresh_bars=int(context.amd_zone_fresh_bars),
        min_zone_reaction=preset_config.min_zone_reaction,
        retest_tolerance_pct=float(context.amd_retest_tolerance_pct),
        max_retest_bars=int(context.amd_max_retest_bars),
        rr_ratio=float(context.rr_ratio),
        trailing_sl_pct=float(context.trailing_sl_pct),
        duplicate_signal_cooldown_bars=preset_config.duplicate_signal_cooldown_bars,
        min_score_conservative=float(context.amd_min_score_conservative),
        min_score_balanced=float(context.amd_min_score_balanced),
        min_score_aggressive=float(context.amd_min_score_aggressive),
        allow_secondary_entries=preset_config.allow_secondary_entries,
        max_trades_per_day=preset_config.max_trades_per_day,
    )
    return dependencies.amd_generator(
        context.candles,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct),
        rr_ratio=float(context.rr_ratio),
        config=config,
    )


STRATEGY_DEFINITIONS: dict[str, StrategyDefinition] = {
    'Breakout': StrategyDefinition(name='Breakout', input_mode='candles', runner=_run_breakout_strategy),
    'Demand Supply (Retest)': StrategyDefinition(name='Demand Supply (Retest)', input_mode='candles', runner=_run_demand_supply_strategy),
    'Demand Supply': StrategyDefinition(name='Demand Supply (Retest)', input_mode='candles', runner=_run_demand_supply_strategy),
    'Indicator': StrategyDefinition(name='Indicator', input_mode='candle_rows', runner=_run_indicator_strategy),
    'One Trade/Day': StrategyDefinition(name='One Trade/Day', input_mode='candle_rows', runner=_run_one_trade_strategy),
    'MTF 5m': StrategyDefinition(name='MTF 5m', input_mode='candle_rows', runner=_run_mtf_strategy),
    'BTST': StrategyDefinition(name='BTST', input_mode='candle_rows', runner=_run_btst_strategy),
    'AMD + FVG + Supply/Demand': StrategyDefinition(name='AMD + FVG + Supply/Demand', input_mode='candles', runner=_run_amd_strategy),
}


def get_strategy_definition(strategy_name: str) -> StrategyDefinition:
    normalized = str(strategy_name or 'Breakout').strip()
    return STRATEGY_DEFINITIONS.get(normalized, STRATEGY_DEFINITIONS['Breakout'])


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
    amd_generator: Callable[..., list[dict[str, object]]] = generate_amd_fvg_sd_trades,
) -> list[dict[str, object]]:
    strategy_name = str(context.strategy or 'Breakout').strip()
    definition = get_strategy_definition(strategy_name)
    dependencies = StrategyDependencies(
        breakout_generator=breakout_generator,
        demand_supply_generator=demand_supply_generator,
        indicator_trade_generator=indicator_trade_generator,
        indicator_row_generator=indicator_row_generator,
        one_trade_generator=one_trade_generator,
        mtf_generator=mtf_generator,
        btst_generator=btst_generator,
        amd_generator=amd_generator,
    )
    rows = definition.runner(context, dependencies)
    return standardize_strategy_rows(rows, strategy_name=strategy_name, symbol=context.symbol)


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
    amd_generator: Callable[..., list[dict[str, object]]] = generate_amd_fvg_sd_trades,
    attach_option_strikes_fn: Callable[..., list[dict[str, object]]] | None = None,
    attach_option_metrics_fn: Callable[..., list[dict[str, object]]] | None = None,
    attach_levels_fn: Callable[..., list[dict[str, object]]] | None = None,
) -> list[dict[str, object]]:
    strategy_name = str(context.strategy or 'Breakout').strip()
    rows = generate_strategy_rows(
        context,
        breakout_generator=breakout_generator,
        demand_supply_generator=demand_supply_generator,
        indicator_trade_generator=indicator_trade_generator,
        indicator_row_generator=indicator_generator,
        one_trade_generator=one_trade_generator,
        mtf_generator=mtf_generator,
        btst_generator=btst_generator,
        amd_generator=amd_generator,
    )
    if attach_levels_fn is not None and strategy_name == 'Indicator':
        rows = attach_levels_fn(rows, rr_ratio=float(context.rr_ratio), trailing_sl_pct=float(context.trailing_sl_pct))
        rows = standardize_strategy_rows(rows, strategy_name=strategy_name, symbol=context.symbol)
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













