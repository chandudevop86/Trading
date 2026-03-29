import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.modules.setdefault('yfinance', types.SimpleNamespace())
sys.modules.setdefault('certifi', types.SimpleNamespace(where=lambda: ''))

from src.strategy_service import STRATEGY_SIGNAL_CONTRACT_VERSION, StrategyContext, generate_strategy_rows, get_strategy_definition
from src.trading_workflows import build_backtest_workflow, run_paper_candidates


class TestTradingWorkflows(unittest.TestCase):
    def test_generate_strategy_rows_supports_btst(self):
        context = StrategyContext(
            strategy='BTST',
            candles=[],
            candle_rows=[],
            capital=100000.0,
            risk_pct=1.0,
            rr_ratio=2.0,
            trailing_sl_pct=0.0,
            symbol='NIFTY',
            cost_bps=10.0,
            fixed_cost_per_trade=5.0,
            max_trades_per_day=2,
        )
        fake_rows = [{'side': 'BUY', 'entry_price': 100.0, 'timestamp': '2026-03-20 09:30:00'}]
        with patch('src.strategy_service.generate_btst_trades', return_value=fake_rows) as mock_btst:
            rows = generate_strategy_rows(context, btst_generator=mock_btst)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['strategy'], 'BTST')
        self.assertEqual(rows[0]['trade_no'], 1)
        self.assertEqual(rows[0]['trade_label'], 'Trade 1')
        self.assertEqual(rows[0]['contract_version'], STRATEGY_SIGNAL_CONTRACT_VERSION)
        self.assertEqual(rows[0]['signal_time'], '2026-03-20 09:30:00')
        mock_btst.assert_called_once()

    def test_strategy_definition_registry_exposes_input_mode(self):
        breakout = get_strategy_definition('Breakout')
        mtf = get_strategy_definition('MTF 5m')
        self.assertEqual(breakout.input_mode, 'candles')
        self.assertEqual(mtf.input_mode, 'candle_rows')

    def test_backtest_and_paper_workflows_are_separate(self):
        output_rows = [{'side': 'BUY', 'entry_price': 100.0, 'timestamp': '2026-03-20 09:30:00', 'strategy': 'BREAKOUT'}]
        backtest = build_backtest_workflow(output_rows, 'Breakout (15m)', 'NIFTY')
        self.assertTrue(backtest.execution_candidates)
        self.assertEqual(backtest.execution_type, 'BACKTEST')
        self.assertIsNone(backtest.execution_result)

        with TemporaryDirectory() as td:
            paper = run_paper_candidates(backtest.execution_candidates, output_path=Path(td) / 'paper.csv', deduplicate=False)
        self.assertEqual(paper.execution_type, 'PAPER')
        self.assertIsNotNone(paper.execution_result)
        self.assertGreaterEqual(len(paper.execution_candidates), 1)


if __name__ == '__main__':
    unittest.main()
