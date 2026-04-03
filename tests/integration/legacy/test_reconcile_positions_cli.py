import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from src import reconcile_positions


class TestReconcilePositionsCli(unittest.TestCase):
    def test_main_prints_position_summary(self):
        rows = [
            {
                'symbol': 'NIFTY',
                'expected_net_qty': 65,
                'broker_net_qty': 65,
                'qty_delta': 0,
                'position_match': 'YES',
            }
        ]
        with patch('src.reconcile_positions.reconcile_live_positions', return_value=rows):
            with patch('sys.argv', ['reconcile_positions', '--live-log', 'data/live_trading_logs_all.csv', '--broker', 'DHAN']):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    reconcile_positions.main()

        output = buffer.getvalue()
        self.assertIn('Position rows: 1', output)
        self.assertIn('NIFTY', output)
        self.assertIn('match=YES', output)


if __name__ == '__main__':
    unittest.main()
