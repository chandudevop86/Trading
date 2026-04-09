import sys
import types
import unittest
from unittest.mock import patch

import pandas as pd

sys.modules.setdefault('yfinance', types.SimpleNamespace())
sys.modules.setdefault('certifi', types.SimpleNamespace(where=lambda: ''))

from src.runtime_models import TradingActionRequest
from src.trading_runtime_service import run_operator_action
from src.runtime_workflow_service import latest_optimizer_gate, run_execution


class TestTradingRuntimeService(unittest.TestCase):
    def _request(self, *, run_requested: bool = False, backtest_requested: bool = False) -> TradingActionRequest:
        return TradingActionRequest(
            strategy='Breakout',
            symbol='NIFTY',
            timeframe='5m',
            capital=20000.0,
            risk_pct=1.0,
            rr_ratio=2.0,
            mode='Balanced',
            broker_choice='Paper',
            run_requested=run_requested,
            backtest_requested=backtest_requested,
        )

    def test_backtest_action_returns_summary_without_live_trade_payload(self):
        candles = pd.DataFrame([{'timestamp': '2026-03-20 09:30:00', 'open': 100.0, 'high': 101.0, 'low': 99.5, 'close': 100.5, 'volume': 1000.0}])
        with patch('src.trading_runtime_service._run_live_strategy', return_value=(candles, [{'side': 'BUY'}], '60d')) as mock_live:
            with patch('src.trading_runtime_service._run_strategy_backtest', return_value={'total_trades': 4, 'total_pnl': 250.0}) as mock_backtest:
                result = run_operator_action(self._request(backtest_requested=True))

        mock_live.assert_called_once()
        mock_backtest.assert_called_once()
        self.assertEqual(result.status, 'Backtest completed')
        self.assertEqual(result.broker_status, 'Backtest mode')
        self.assertEqual(result.trades, [])
        self.assertEqual(result.active_summary['total_trades'], 4)
        self.assertEqual(result.paper_summary, {})
        self.assertEqual(result.execution_messages, [])
        self.assertEqual(result.todays_trades, 0)

    def test_run_action_keeps_trade_payload_and_execution_messages(self):
        candles = pd.DataFrame([{'timestamp': '2026-03-20 09:30:00', 'open': 100.0, 'high': 101.0, 'low': 99.5, 'close': 100.5, 'volume': 1000.0}])
        trades = [{'side': 'BUY', 'entry_price': 100.5}]
        with patch('src.trading_runtime_service._run_live_strategy', return_value=(candles, trades, '60d')) as mock_live:
            with patch('src.trading_runtime_service._run_execution', return_value=(object(), [('success', '1 trade executed')], 'Paper broker active')) as mock_exec:
                with patch('src.trading_runtime_service.paper_execution_summary', return_value={'total_trades': 1, 'total_pnl': 100.0}) as mock_summary:
                    with patch('src.trading_runtime_service.todays_trade_count', return_value=1) as mock_count:
                        result = run_operator_action(self._request(run_requested=True))

        mock_live.assert_called_once()
        mock_exec.assert_called_once()
        mock_summary.assert_called_once()
        mock_count.assert_called_once()
        self.assertEqual(result.status, 'Run completed')
        self.assertEqual(result.trades, trades)
        self.assertEqual(result.execution_messages, [('success', '1 trade executed')])
        self.assertEqual(result.active_summary['total_trades'], 1)
        self.assertEqual(result.todays_trades, 1)


    def test_latest_optimizer_gate_requires_deployment_ready_yes(self):
        fake_rows = [
            {'strategy': 'BREAKOUT', 'deployment_ready': 'NO', 'deployment_blockers': 'NEGATIVE_EXPECTANCY', 'optimizer_rank': 1},
        ]
        with patch('src.runtime_workflow_service.load_current_rows', return_value=fake_rows):
            with patch('src.runtime_workflow_service.load_latest_batch_rows', return_value=[]):
                ready, reason = latest_optimizer_gate('Breakout')

        self.assertFalse(ready)
        self.assertIn('live deployment locked:', reason)
        self.assertIn('NEGATIVE_EXPECTANCY', reason)

    def test_run_execution_blocks_live_deployment_when_optimizer_gate_fails(self):
        request = TradingActionRequest(
            strategy='Breakout',
            symbol='NIFTY',
            timeframe='5m',
            capital=20000.0,
            risk_pct=1.0,
            rr_ratio=2.0,
            mode='Balanced',
            broker_choice='Dhan Live',
            run_requested=True,
            backtest_requested=False,
        )
        trades = [{'side': 'BUY', 'entry_price': 100.5, 'stop_loss': 99.5, 'target_price': 102.5, 'signal_time': '2026-03-20 09:30:00', 'strategy': 'BREAKOUT'}]
        candles = pd.DataFrame([{'timestamp': '2026-03-20 09:30:00', 'open': 100.0, 'high': 101.0, 'low': 99.5, 'close': 100.5, 'volume': 1000.0}])

        with patch('src.runtime_workflow_service.latest_optimizer_gate', return_value=(False, 'live deployment locked: NEGATIVE_EXPECTANCY')):
            _, messages, broker_status = run_execution(request, trades, candles)

        self.assertEqual(broker_status, 'Live broker blocked by optimizer gate')
        self.assertEqual(messages, [('warning', 'Live blocked: live deployment locked: NEGATIVE_EXPECTANCY')])
    def test_run_action_returns_safe_error_payload_when_runtime_fails(self):
        with patch('src.trading_runtime_service._run_live_strategy', side_effect=ValueError('bad candle payload')):
            result = run_operator_action(self._request(run_requested=True))

        self.assertEqual(result.broker_status, 'Runtime error')
        self.assertEqual(result.trades, [])
        self.assertEqual(result.execution_messages, [('error', 'bad candle payload')])
        self.assertIn('Run failed:', result.status)
        self.assertEqual(result.todays_trades, 0)

    def test_backtest_action_returns_backtest_failure_payload_when_runtime_fails(self):
        with patch('src.trading_runtime_service._run_live_strategy', side_effect=ValueError('bad candle payload')):
            result = run_operator_action(self._request(backtest_requested=True))

        self.assertEqual(result.broker_status, 'Runtime error')
        self.assertEqual(result.trades, [])
        self.assertEqual(result.execution_messages, [('error', 'bad candle payload')])
        self.assertIn('Backtest failed:', result.status)
        self.assertEqual(result.todays_trades, 0)


if __name__ == '__main__':
    unittest.main()




