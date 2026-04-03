import inspect
import sys
import types
import unittest
from unittest.mock import Mock

sys.modules.setdefault('yfinance', types.SimpleNamespace())
sys.modules.setdefault('certifi', types.SimpleNamespace(where=lambda: ''))

from src.amd_fvg_sd_bot import ConfluenceConfig, generate_trades as generate_amd_trades
from src.breakout_bot import BreakoutConfig, generate_trades as generate_breakout_trades
from src.btst_bot import BtstConfig, generate_trades as generate_btst_trades
from src.demand_supply_bot import DemandSupplyConfig, generate_trades as generate_demand_supply_trades
from src.indicator_bot import IndicatorConfig, generate_trades as generate_indicator_trades
from src.mtf_trade_bot import MtfTradeConfig, generate_trades as generate_mtf_trades
from src.one_trade_day import generate_trades as generate_one_trade_day_trades
from src.strategy_service import StrategyContext, generate_strategy_rows


class TestStrategyInterfaces(unittest.TestCase):
    def test_active_strategy_generators_share_common_signature_prefix(self):
        cases = [
            (generate_breakout_trades, BreakoutConfig),
            (generate_demand_supply_trades, DemandSupplyConfig),
            (generate_indicator_trades, IndicatorConfig),
            (generate_one_trade_day_trades, IndicatorConfig),
            (generate_mtf_trades, MtfTradeConfig),
            (generate_btst_trades, BtstConfig),
            (generate_amd_trades, ConfluenceConfig),
        ]

        for func, config_type in cases:
            params = list(inspect.signature(func).parameters.values())
            self.assertGreaterEqual(len(params), 5, func.__module__)
            self.assertEqual(params[1].name, 'capital', func.__module__)
            self.assertEqual(params[2].name, 'risk_pct', func.__module__)
            self.assertEqual(params[3].name, 'rr_ratio', func.__module__)
            self.assertEqual(params[3].default, 2.0, func.__module__)
            self.assertEqual(params[4].name, 'config', func.__module__)
            self.assertIsNone(params[4].default, func.__module__)
            self.assertIn(config_type.__name__, str(params[4].annotation), func.__module__)
            for extra in params[5:]:
                self.assertEqual(extra.kind, inspect.Parameter.KEYWORD_ONLY, f'{func.__module__}:{extra.name}')

    def test_strategy_service_routes_mtf_through_shared_config_contract(self):
        context = StrategyContext(
            strategy='MTF 5m',
            candles=[],
            candle_rows=[],
            capital=100000.0,
            risk_pct=1.0,
            rr_ratio=2.0,
            trailing_sl_pct=0.25,
            symbol='NIFTY',
            cost_bps=10.0,
            fixed_cost_per_trade=5.0,
            max_daily_loss=1500.0,
            mtf_ema_period=4,
            mtf_setup_mode='bos',
            mtf_retest_strength=False,
        )
        mock_mtf = Mock(return_value=[])

        generate_strategy_rows(context, mtf_generator=mock_mtf)

        kwargs = mock_mtf.call_args.kwargs
        self.assertEqual(kwargs['rr_ratio'], 2.0)
        self.assertIsInstance(kwargs['config'], MtfTradeConfig)
        self.assertEqual(kwargs['config'].ema_period, 4)
        self.assertFalse(kwargs['config'].require_retest_strength)

    def test_strategy_service_routes_btst_through_shared_config_contract(self):
        context = StrategyContext(
            strategy='BTST',
            candles=[],
            candle_rows=[],
            capital=100000.0,
            risk_pct=1.0,
            rr_ratio=2.5,
            trailing_sl_pct=0.0,
            symbol='NIFTY',
            cost_bps=10.0,
            fixed_cost_per_trade=5.0,
            max_daily_loss=1000.0,
            max_trades_per_day=2,
        )
        mock_btst = Mock(return_value=[])

        generate_strategy_rows(context, btst_generator=mock_btst)

        kwargs = mock_btst.call_args.kwargs
        self.assertEqual(kwargs['rr_ratio'], 2.5)
        self.assertIsInstance(kwargs['config'], BtstConfig)
        self.assertTrue(kwargs['config'].allow_stbt)
        self.assertEqual(kwargs['config'].max_trades_per_day, 2)


if __name__ == '__main__':
    unittest.main()

