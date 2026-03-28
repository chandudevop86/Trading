import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.modules.setdefault('yfinance', types.SimpleNamespace())
sys.modules.setdefault('certifi', types.SimpleNamespace(where=lambda: ''))

from src.operational_daemon import _build_cycle_telegram_message, execute_trading_cycle
from src.runtime_config import RuntimeConfig, TelegramRuntimeConfig


class TestOperationalDaemon(unittest.TestCase):
    def test_build_cycle_telegram_message_includes_core_status(self):
        config = RuntimeConfig(
            telegram=TelegramRuntimeConfig(enabled=True, token='token', chat_id='chat'),
        )
        message = _build_cycle_telegram_message(
            config,
            {
                'candles': 20,
                'trades': 3,
                'candidates': 2,
                'execution_messages': [('success', '1 trade executed')],
                'executed_rows': [],
            },
        )
        self.assertIn('Trading cycle update', message)
        self.assertIn('Strategy: Breakout', message)
        self.assertIn('Execution status: 1 trade executed', message)
        self.assertIn('Paper readiness:', message)

    def test_execute_trading_cycle_sends_telegram_summary_when_enabled(self):
        config = RuntimeConfig(
            telegram=TelegramRuntimeConfig(enabled=True, token='token', chat_id='chat', notify_on_success=True),
        )
        candles = pd.DataFrame([
            {'timestamp': '2026-03-28 09:30:00', 'open': 100.0, 'high': 101.0, 'low': 99.5, 'close': 100.5, 'volume': 1000.0},
        ])
        trades = [
            {'strategy': 'Breakout', 'symbol': '^NSEI', 'side': 'BUY', 'entry_price': 100.5, 'stop_loss': 99.5, 'target_price': 102.0, 'signal_time': '2026-03-28 09:30:00'},
        ]
        execution_result = types.SimpleNamespace(
            executed_rows=[{'strategy': 'Breakout', 'symbol': '^NSEI', 'side': 'BUY', 'entry_time': '2026-03-28 09:30:00', 'exit_time': '2026-03-28 09:35:00', 'pnl': 50.0, 'execution_status': 'CLOSED'}],
            blocked_rows=[],
            skipped_rows=[],
        )
        with patch('src.operational_daemon.fetch_ohlcv_data', return_value=candles):
            with patch('src.operational_daemon.run_strategy', return_value=trades):
                with patch('src.operational_daemon.write_rows'):
                    with patch('src.operational_daemon.build_execution_candidates', return_value=trades):
                        with patch('src.operational_daemon.execute_paper_trades', return_value=execution_result):
                            with patch('src.operational_daemon.close_paper_trades'):
                                with patch('src.operational_daemon.summarize_trade_log'):
                                    with patch('src.operational_daemon.execution_result_summary', return_value=[('success', '1 trade executed')]):
                                        with patch('src.operational_daemon.send_telegram_message') as mock_send:
                                            with patch('src.operational_daemon.sync_path_to_s3_if_enabled'):
                                                cycle = execute_trading_cycle(config)
        self.assertEqual(cycle['candles'], 1)
        mock_send.assert_called_once()
        sent_message = mock_send.call_args.args[2]
        self.assertIn('Trading cycle update', sent_message)
        self.assertIn('1 trade executed', sent_message)


if __name__ == '__main__':
    unittest.main()
