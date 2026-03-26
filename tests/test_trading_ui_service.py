import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.modules.setdefault('streamlit', types.SimpleNamespace(
    set_page_config=lambda **kwargs: None,
    markdown=lambda *args, **kwargs: None,
    columns=lambda count: [types.SimpleNamespace(metric=lambda *a, **k: None) for _ in range(count)],
    caption=lambda *args, **kwargs: None,
))

from src.trading_runtime_service import TradingActionRequest
from src.trading_ui_service import build_request, initialize_ui_runtime, log_ui_event


class TestTradingUiService(unittest.TestCase):
    def test_build_request_returns_runtime_request(self):
        request = build_request('Breakout', 'NIFTY', '5m', 20000.0, 1.0, 2.0, 'Balanced', 'Paper', True, False)
        self.assertIsInstance(request, TradingActionRequest)
        self.assertEqual(request.strategy, 'Breakout')
        self.assertTrue(request.run_requested)
        self.assertFalse(request.backtest_requested)

    def test_initialize_ui_runtime_creates_output_and_log_paths(self):
        with TemporaryDirectory() as td:
            base = Path(td)
            output_path = base / 'data' / 'output.csv'
            log_path = base / 'logs' / 'app.log'
            initialize_ui_runtime([output_path], [log_path])
            self.assertTrue(output_path.exists())
            self.assertTrue(log_path.exists())

    def test_log_ui_event_appends_message(self):
        with TemporaryDirectory() as td:
            log_path = Path(td) / 'logs' / 'app.log'
            log_ui_event(log_path, 'hello world')
            self.assertIn('hello world', log_path.read_text(encoding='utf-8'))


if __name__ == '__main__':
    unittest.main()
