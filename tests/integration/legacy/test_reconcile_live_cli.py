import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from src import reconcile_live


class TestReconcileLiveCli(unittest.TestCase):
    def test_main_prints_reconciled_summary(self):
        rows = [
            {
                'strategy': 'BREAKOUT',
                'broker_order_id': 'ORD123',
                'broker_status': 'TRADED',
                'execution_status': 'FILLED',
            }
        ]
        with patch('src.reconcile_live.reconcile_live_trades', return_value=rows):
            with patch('sys.argv', ['reconcile_live', '--live-log', 'data/live_trading_logs_all.csv', '--broker', 'DHAN']):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    reconcile_live.main()

        output = buffer.getvalue()
        self.assertIn('Reconciled rows: 1', output)
        self.assertIn('ORD123', output)
        self.assertIn('FILLED', output)


if __name__ == '__main__':
    unittest.main()
