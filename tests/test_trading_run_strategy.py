import sys
import types
import unittest
from unittest.mock import patch

import pandas as pd

sys.modules.setdefault("yfinance", types.SimpleNamespace())

from src.Trading import run_strategy
from src.amd_fvg_sd_bot import ConfluenceConfig
from src.breakout_bot import BreakoutConfig
from src.demand_supply_bot import DemandSupplyConfig


class TestTradingRunStrategy(unittest.TestCase):
    def setUp(self):
        self.candles = pd.DataFrame(
            [
                {"timestamp": "2026-03-20 09:15:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000},
                {"timestamp": "2026-03-20 09:30:00", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 1000},
            ]
        )
        self.common_kwargs = dict(
            candles=self.candles,
            capital=100000,
            risk_pct=1.0,
            rr_ratio=2.0,
            trailing_sl_pct=1.0,
            symbol="NIFTY",
            strike_step=50,
            moneyness="ATM",
            strike_steps=0,
            fetch_option_metrics=False,
            mtf_ema_period=3,
            mtf_setup_mode="either",
            mtf_retest_strength=True,
            mtf_max_trades_per_day=3,
        )

    @patch("src.Trading.attach_option_strikes")
    @patch("src.Trading.generate_breakout_trades")
    def test_breakout_strategy_routes_and_normalizes(self, mock_breakout, mock_attach):
        mock_breakout.return_value = [
            {
                "side": "BUY",
                "entry_price": 101.0,
                "stop_loss": 99.0,
                "target_price": 105.0,
                "timestamp": "2026-03-20 09:30:00",
            }
        ]
        mock_attach.side_effect = lambda rows, **_: rows

        rows = run_strategy(strategy="Breakout", **self.common_kwargs)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["strategy"], "BREAKOUT")
        self.assertEqual(rows[0]["symbol"], "NIFTY")
        self.assertEqual(rows[0]["trade_no"], 1)
        self.assertEqual(rows[0]["trade_label"], "Trade 1")
        self.assertEqual(rows[0]["entry_time"], "2026-03-20 09:30:00")
        mock_breakout.assert_called_once()
        kwargs = mock_breakout.call_args.kwargs
        self.assertEqual(kwargs["capital"], 100000.0)
        self.assertEqual(kwargs["risk_pct"], 0.01)
        self.assertEqual(kwargs["rr_ratio"], 2.0)
        self.assertIsInstance(kwargs["config"], BreakoutConfig)
        self.assertTrue(kwargs["config"].require_vwap_alignment)
        self.assertTrue(kwargs["config"].allow_secondary_entries)
        self.assertEqual(kwargs["config"].duplicate_signal_cooldown_bars, 8)
        self.assertEqual(kwargs["config"].max_trades_per_day, 1)
        mock_attach.assert_called_once()

    @patch("src.Trading.attach_option_strikes")
    @patch("src.Trading.generate_demand_supply_trades")
    def test_demand_supply_strategy_receives_risk_inputs(self, mock_demand_supply, mock_attach):
        mock_demand_supply.return_value = [
            {
                "side": "SELL",
                "entry_price": 100.0,
                "stop_loss": 102.0,
                "target_price": 96.0,
                "timestamp": "2026-03-20 09:30:00",
            }
        ]
        mock_attach.side_effect = lambda rows, **_: rows

        rows = run_strategy(strategy="Demand Supply", **self.common_kwargs)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["strategy"], "DEMAND_SUPPLY")
        mock_demand_supply.assert_called_once()
        kwargs = mock_demand_supply.call_args.kwargs
        self.assertEqual(kwargs["capital"], 100000.0)
        self.assertEqual(kwargs["risk_pct"], 0.01)
        self.assertEqual(kwargs["rr_ratio"], 2.0)
        self.assertIsInstance(kwargs["config"], DemandSupplyConfig)
        self.assertTrue(kwargs["config"].require_vwap_alignment)
        self.assertTrue(kwargs["config"].require_trend_bias)
        self.assertEqual(kwargs["config"].duplicate_signal_cooldown_bars, 12)
        self.assertEqual(kwargs["config"].max_trades_per_day, 1)
        mock_attach.assert_called_once()

    @patch("src.Trading._attach_option_metrics")
    @patch("src.Trading.attach_option_strikes")
    @patch("src.Trading.generate_amd_fvg_sd_trades")
    def test_amd_strategy_uses_strict_confluence_config(self, mock_amd, mock_attach, mock_metrics):
        mock_amd.return_value = [
            {
                "side": "SELL",
                "entry_price": 100.0,
                "stop_loss": 102.0,
                "target_price": 96.0,
                "timestamp": "2026-03-20 09:30:00",
            }
        ]
        mock_attach.side_effect = lambda rows, **_: rows
        mock_metrics.side_effect = lambda rows, *_args, **_kwargs: rows

        rows = run_strategy(strategy="AMD + FVG + Supply/Demand", **self.common_kwargs)

        self.assertEqual(len(rows), 1)
        mock_amd.assert_called_once()
        kwargs = mock_amd.call_args.kwargs
        self.assertIsInstance(kwargs["config"], ConfluenceConfig)
        self.assertTrue(kwargs["config"].require_vwap_alignment)
        self.assertTrue(kwargs["config"].require_trend_alignment)
        self.assertTrue(kwargs["config"].require_retest_confirmation)
        self.assertEqual(kwargs["config"].max_trades_per_day, 1)
        self.assertEqual(kwargs["config"].duplicate_signal_cooldown_bars, 12)
        self.assertEqual(kwargs["config"].morning_session_start, "09:20")
        self.assertFalse(kwargs["config"].allow_afternoon_session)
        mock_attach.assert_called_once()
        mock_metrics.assert_called_once()

    @patch("src.Trading.attach_option_strikes")
    @patch("src.Trading.generate_indicator_rows")
    def test_indicator_strategy_maps_market_signal_to_side(self, mock_indicator, mock_attach):
        mock_indicator.return_value = [
            {
                "timestamp": "2026-03-20 09:30:00",
                "close": 101.5,
                "market_signal": "BULLISH_TREND",
            }
        ]
        mock_attach.side_effect = lambda rows, **_: rows

        rows = run_strategy(strategy="Indicator", **self.common_kwargs)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["side"], "BUY")
        self.assertEqual(rows[0]["entry_price"], 101.5)
        self.assertEqual(rows[0]["strategy"], "INDICATOR")
        mock_attach.assert_called_once()

    @patch("src.Trading.attach_option_strikes")
    @patch("src.Trading.generate_mtf_trade_trades")
    def test_mtf_strategy_routes_through_app_entry(self, mock_mtf, mock_attach):
        mock_mtf.return_value = [
            {
                "side": "BUY",
                "entry_price": 101.0,
                "stop_loss": 100.0,
                "target_price": 103.0,
                "timestamp": "2026-03-20 09:30:00",
            }
        ]
        mock_attach.side_effect = lambda rows, **_: rows

        rows = run_strategy(strategy="MTF 5m", **self.common_kwargs)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["strategy"], "MTF_5M")
        mock_mtf.assert_called_once()
        kwargs = mock_mtf.call_args.kwargs
        self.assertEqual(kwargs["max_trades_per_day"], 1)
        self.assertTrue(kwargs["require_retest_strength"])
        mock_attach.assert_called_once()


if __name__ == "__main__":
    unittest.main()
