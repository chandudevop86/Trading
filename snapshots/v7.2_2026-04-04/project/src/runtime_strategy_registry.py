from __future__ import annotations

from types import ModuleType
from typing import Any, Callable

StrategyGenerator = Callable[..., list[dict[str, object]]]
OptionMetricsFn = Callable[[list[dict[str, object]], str, bool], list[dict[str, object]]]


def configure_runtime_strategy_dependencies(
    runtime_module: ModuleType,
    *,
    breakout_generator: StrategyGenerator,
    demand_supply_generator: StrategyGenerator,
    amd_generator: StrategyGenerator,
    indicator_row_generator: StrategyGenerator,
    mtf_generator: StrategyGenerator,
    attach_option_strikes_fn: Callable[..., list[dict[str, object]]],
    attach_option_metrics_fn: OptionMetricsFn,
) -> ModuleType:
    runtime_module.generate_breakout_trades = breakout_generator
    runtime_module.generate_demand_supply_trades = demand_supply_generator
    runtime_module.generate_amd_fvg_sd_trades = amd_generator
    runtime_module.generate_indicator_rows = indicator_row_generator
    runtime_module.generate_mtf_trade_trades = mtf_generator
    runtime_module.attach_option_strikes = attach_option_strikes_fn
    runtime_module._attach_option_metrics = attach_option_metrics_fn
    return runtime_module


def run_configured_runtime_strategy(runtime_module: ModuleType, **kwargs: Any) -> list[dict[str, object]]:
    return runtime_module.run_strategy(**kwargs)
