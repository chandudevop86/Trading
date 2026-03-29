import sys
import types
import unittest
from unittest.mock import Mock

from src.runtime_strategy_registry import configure_runtime_strategy_dependencies, run_configured_runtime_strategy


class TestRuntimeStrategyRegistry(unittest.TestCase):
    def test_configure_runtime_strategy_dependencies_assigns_generators(self):
        runtime_module = types.SimpleNamespace(_attach_option_metrics=None)
        breakout = Mock()
        demand_supply = Mock()
        amd = Mock()
        indicator = Mock()
        mtf = Mock()
        attach_strikes = Mock()
        attach_metrics = Mock()

        configured = configure_runtime_strategy_dependencies(
            runtime_module,
            breakout_generator=breakout,
            demand_supply_generator=demand_supply,
            amd_generator=amd,
            indicator_row_generator=indicator,
            mtf_generator=mtf,
            attach_option_strikes_fn=attach_strikes,
            attach_option_metrics_fn=attach_metrics,
        )

        self.assertIs(configured.generate_breakout_trades, breakout)
        self.assertIs(configured.generate_demand_supply_trades, demand_supply)
        self.assertIs(configured.generate_amd_fvg_sd_trades, amd)
        self.assertIs(configured.generate_indicator_rows, indicator)
        self.assertIs(configured.generate_mtf_trade_trades, mtf)
        self.assertIs(configured.attach_option_strikes, attach_strikes)
        self.assertIs(configured._attach_option_metrics, attach_metrics)

    def test_run_configured_runtime_strategy_delegates_to_runtime_module(self):
        runtime_module = types.SimpleNamespace(run_strategy=Mock(return_value=[{'side': 'BUY'}]))

        rows = run_configured_runtime_strategy(runtime_module, strategy='Breakout')

        self.assertEqual(rows, [{'side': 'BUY'}])
        runtime_module.run_strategy.assert_called_once_with(strategy='Breakout')


if __name__ == '__main__':
    unittest.main()
