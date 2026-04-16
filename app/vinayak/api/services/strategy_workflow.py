from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal

from vinayak.core.statuses import ValidationStatus
from vinayak.execution.contracts import normalize_candidate_contract, validate_candidate_contract
from vinayak.observability.observability_logger import log_event
from vinayak.strategies.amd.service import ConfluenceConfig, run_amd_strategy as run_native_amd_strategy
from vinayak.strategies.breakout.service import Candle, run_breakout_strategy as run_native_breakout_strategy
from vinayak.strategies.btst.service import BtstConfig, run_btst_strategy as run_native_btst_strategy
from vinayak.strategies.common.base import StrategySignal, TradeSignal
from vinayak.strategies.demand_supply.service import run_demand_supply_strategy as run_native_demand_supply_strategy
from vinayak.strategies.indicator.service import IndicatorConfig, run_indicator_strategy as run_native_indicator_strategy
from vinayak.strategies.mtf.service import run_mtf_strategy as run_native_mtf_strategy
from vinayak.strategies.one_trade_day.service import run_one_trade_day_strategy as run_native_one_trade_day_strategy

STRATEGY_SIGNAL_CONTRACT_VERSION = 'strict_trade_candidate_v1'
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
    max_position_value: float | None = None
    max_open_positions: int | None = None
    max_symbol_exposure_pct: float | None = None
    max_portfolio_exposure_pct: float | None = None
    max_open_risk_pct: float | None = None
    kill_switch_enabled: bool = False
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
    pass


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


def _strategy_signal_to_row(signal: StrategySignal) -> dict[str, object]:
    metadata = dict(signal.metadata or {})
    quantity = int(metadata.pop('quantity', 0) or 0)
    timestamp = signal.signal_time.strftime('%Y-%m-%d %H:%M:%S')
    return {
        'strategy': signal.strategy_name,
        'strategy_name': signal.strategy_name,
        'symbol': signal.symbol,
        'side': signal.side,
        'entry': signal.entry_price,
        'entry_price': signal.entry_price,
        'stop_loss': signal.stop_loss,
        'target': signal.target_price,
        'target_price': signal.target_price,
        'timestamp': timestamp,
        'entry_time': timestamp,
        'quantity': quantity,
        **metadata,
    }


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
    canonical_strategy = _canonical_strategy_name(strategy_name)
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
        item['strategy_name'] = str(item.get('strategy_name') or canonical_strategy)
        item['setup_type'] = str(item.get('setup_type') or item.get('zone_type') or canonical_strategy)
        item['timeframe'] = str(item.get('timeframe') or item.get('interval') or '')
        item['validation_status'] = str(item.get('validation_status') or ValidationStatus.PENDING).upper()
        item['validation_score'] = round(_safe_float(item.get('validation_score', item.get('score', 0.0))) or 0.0, 2)
        item['validation_reasons'] = list(item.get('validation_reasons', [])) if isinstance(item.get('validation_reasons', []), list) else []
        item['execution_allowed'] = bool(item.get('execution_allowed', False))
        entry_numeric = _safe_float(item.get('entry'))
        stop_numeric = _safe_float(item.get('stop_loss'))
        risk_per_unit = abs(entry_numeric - stop_numeric) if entry_numeric is not None and stop_numeric is not None else 0.0
        item['risk_per_unit'] = round(risk_per_unit, 4)
        standardized.append(normalize_candidate_contract(item, symbol=symbol, strategy_name=strategy_name, timeframe=item['timeframe']))
    return standardized


def validate_strategy_output_rows(
    rows: list[dict[str, object]],
    *,
    strategy_name: str,
    symbol: str,
) -> list[dict[str, object]]:
    valid_rows: list[dict[str, object]] = []
    invalid_rows: list[dict[str, object]] = []
    for row in rows:
        is_valid, reasons, normalized = validate_candidate_contract(dict(row))
        if is_valid:
            valid_rows.append(normalized)
            continue
        invalid_rows.append({
            "trade_id": normalized.get("trade_id", ""),
            "symbol": normalized.get("symbol", symbol),
            "strategy_name": normalized.get("strategy_name", strategy_name),
            "reasons": list(reasons),
            "normalized": dict(normalized),
        })
    if invalid_rows:
        log_event(
            component="strategy_workflow",
            event_name="invalid_strategy_candidates_filtered",
            symbol=symbol,
            strategy=strategy_name,
            severity="WARNING",
            message="Filtered invalid strategy candidate rows before downstream execution.",
            context_json={"invalid_rows": invalid_rows, "invalid_count": len(invalid_rows)},
        )
    return valid_rows


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
    signals = run_native_breakout_strategy(
        candles=context.candle_rows,
        symbol=context.symbol,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
    )
    return [_strategy_signal_to_row(signal) for signal in signals]


def _run_demand_supply_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    signals = run_native_demand_supply_strategy(
        candles=context.candle_rows,
        symbol=context.symbol,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
    )
    return [_strategy_signal_to_row(signal) for signal in signals]


def _run_indicator_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    signals = run_native_indicator_strategy(context.candle_rows, symbol=context.symbol, config=IndicatorConfig())
    return [_strategy_signal_to_row(signal) for signal in signals]


def _run_one_trade_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    signals = run_native_one_trade_day_strategy(
        candles=context.candle_rows,
        symbol=context.symbol,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
        entry_cutoff_hhmm=str(context.entry_cutoff),
        config=IndicatorConfig(),
    )
    return [_strategy_signal_to_row(signal) for signal in signals]


def _run_mtf_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    signals = run_native_mtf_strategy(
        candles=context.candle_rows,
        symbol=context.symbol,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
        ema_period=int(context.mtf_ema_period),
        setup_mode=str(context.mtf_setup_mode),
        require_retest_strength=bool(context.mtf_retest_strength),
    )
    return [_strategy_signal_to_row(signal) for signal in signals]


def _run_btst_strategy(context: StrategyContext, dependencies: StrategyDependencies) -> list[dict[str, object]]:
    signals = run_native_btst_strategy(
        candles=context.candle_rows,
        symbol=context.symbol,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct) / 100.0,
        rr_ratio=float(context.rr_ratio),
        config=BtstConfig(allow_stbt=True),
    )
    return [_strategy_signal_to_row(signal) for signal in signals]


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
        zone_fresh_bars=int(context.amd_zone_fresh_bars),
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
    signals = run_native_amd_strategy(
        data=context.candles,
        symbol=context.symbol,
        capital=float(context.capital),
        risk_pct=float(context.risk_pct),
        rr_ratio=float(context.rr_ratio),
        config=config,
    )
    return [_strategy_signal_to_row(signal) for signal in signals]


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


def generate_strategy_rows(context: StrategyContext, **dependency_overrides: Callable[..., list[dict[str, object]]]) -> list[dict[str, object]]:
    strategy_name = str(context.strategy or 'Breakout').strip()
    definition = get_strategy_definition(strategy_name)
    dependencies = StrategyDependencies(**{key: value for key, value in dependency_overrides.items() if key in StrategyDependencies.__dataclass_fields__})
    rows = definition.runner(context, dependencies)
    standardized = standardize_strategy_rows(rows, strategy_name=strategy_name, symbol=context.symbol)
    return validate_strategy_output_rows(standardized, strategy_name=strategy_name, symbol=context.symbol)


def run_strategy_workflow(
    context: StrategyContext,
    *,
    attach_levels_fn: Callable[[list[dict[str, object]], float, float], list[dict[str, object]]] | None = None,
    attach_option_strikes_fn: Callable[..., list[dict[str, object]]] | None = None,
    attach_option_metrics_fn: Callable[..., list[dict[str, object]]] | None = None,
    **dependency_overrides: Callable[..., list[dict[str, object]]],
) -> list[dict[str, object]]:
    rows = generate_strategy_rows(context, **dependency_overrides)
    strategy_name = str(context.strategy or 'Breakout').strip()
    if strategy_name == 'Indicator' and attach_levels_fn is not None:
        rows = attach_levels_fn(rows, float(context.rr_ratio), float(context.trailing_sl_pct))
        rows = standardize_strategy_rows(rows, strategy_name=strategy_name, symbol=context.symbol)
    if attach_option_strikes_fn is not None and attach_option_metrics_fn is not None:
        rows = enrich_actionable_rows(
            rows,
            symbol=context.symbol,
            strike_step=context.strike_step,
            moneyness=context.moneyness,
            strike_steps=context.strike_steps,
            fetch_option_metrics=context.fetch_option_metrics,
            attach_option_strikes_fn=attach_option_strikes_fn,
            attach_option_metrics_fn=attach_option_metrics_fn,
        )
    return rows


__all__ = [
    'Candle',
    'StrategyContext',
    'StrategyDefinition',
    'StrategyDependencies',
    'TradeSignal',
    'generate_strategy_rows',
    'get_strategy_definition',
    'run_strategy_workflow',
    'standardize_strategy_rows',
]

