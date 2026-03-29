import sys
import types
import unittest
from unittest.mock import patch

import pandas as pd

sys.modules.setdefault('yfinance', types.SimpleNamespace())
sys.modules.setdefault('certifi', types.SimpleNamespace(where=lambda: ''))
sys.modules.setdefault(
    'streamlit',
    types.SimpleNamespace(
        set_page_config=lambda **kwargs: None,
        markdown=lambda *args, **kwargs: None,
        columns=lambda count: [],
        tabs=lambda labels: [],
        caption=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        success=lambda *args, **kwargs: None,
        info=lambda *args, **kwargs: None,
        session_state={},
    ),
)

from src.runtime_models import TradingActionResult
import src.Trading as trading_page


class _FakeColumn:
    def __init__(self, st):
        self._st = st

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def text_input(self, label, value=''):
        return self._st.text_input(label, value=value)

    def selectbox(self, label, options, index=0):
        return self._st.selectbox(label, options, index=index)

    def number_input(self, label, min_value=None, value=0.0, step=None):
        return self._st.number_input(label, min_value=min_value, value=value, step=step)

    def button(self, label, **kwargs):
        return self._st.button(label, **kwargs)

    def metric(self, *args, **kwargs):
        return None


class _FakeStreamlit:
    def __init__(self, *, run_clicked=False, backtest_clicked=False):
        self.session_state = {'backtest_summary': {'stale': True}}
        self._run_clicked = run_clicked
        self._backtest_clicked = backtest_clicked
        self.errors = []
        self.warnings = []
        self.successes = []
        self.infos = []
        self.dataframes = []

    def set_page_config(self, **kwargs):
        return None

    def markdown(self, *args, **kwargs):
        return None

    def caption(self, *args, **kwargs):
        return None

    def columns(self, count):
        return [_FakeColumn(self) for _ in range(count)]

    def tabs(self, labels):
        return [_FakeColumn(self) for _ in labels]

    def expander(self, label, **kwargs):
        return _FakeColumn(self)

    def text_input(self, label, value=''):
        return value

    def selectbox(self, label, options, index=0):
        return options[index]

    def number_input(self, label, min_value=None, value=0.0, step=None):
        return value

    def button(self, label, **kwargs):
        if label == 'Run':
            return self._run_clicked
        if label == 'Backtest':
            return self._backtest_clicked
        return False

    def error(self, message):
        self.errors.append(str(message))

    def warning(self, message):
        self.warnings.append(str(message))

    def success(self, message):
        self.successes.append(str(message))

    def info(self, message):
        self.infos.append(str(message))

    def dataframe(self, data, **kwargs):
        self.dataframes.append((data, kwargs))


class TestTradingPage(unittest.TestCase):
    def test_build_scorecard_rows_uses_validation_summary(self):
        rows = trading_page._build_scorecard_rows(
            {
                'total_trades': 140,
                'avg_trades_per_day': 1.1,
                'duplicate_rejections': 0,
                'risk_rule_rejections': 0,
                'profit_factor': 1.45,
                'expectancy_per_trade': 12.0,
                'max_drawdown_pct': 7.5,
                'deployment_ready': 'YES',
                'sample_window_passed': 'YES',
            },
            status='Backtest completed',
            todays_trades=1,
        )

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]['area'], 'Trade Quality')
        self.assertGreaterEqual(float(rows[1]['score']), 8.0)
        self.assertIn('passed current gates', rows[1]['current issue'])

    def test_build_validation_snapshot_uses_expected_gates(self):
        frame = trading_page._build_validation_snapshot(
            {
                'total_trades': 140,
                'sample_window_passed': 'YES',
                'expectancy_per_trade': 5.5,
                'profit_factor': 1.4,
                'max_drawdown_pct': 7.2,
                'duplicate_rejections': 0,
                'deployment_ready': 'YES',
            }
        )

        self.assertEqual(list(frame['metric'])[:2], ['Sample size', 'Expectancy'])
        self.assertIn('PASS', set(frame['status']))

    def _result(self, *, status='Run completed', backtest_summary=None, execution_messages=None):
        return TradingActionResult(
            candles=pd.DataFrame(),
            trades=[{'side': 'BUY', 'strategy': 'BREAKOUT', 'entry_price': 100.5, 'entry': 100.5, 'stop_loss': 99.0, 'target': 103.5, 'score': 8.1, 'reason': 'retest_vwap_zone'}],
            period='60d',
            status=status,
            broker_status='Paper broker active',
            active_summary={'total_trades': 1},
            backtest_summary=backtest_summary or {},
            paper_summary={'total_trades': 1},
            market_data_summary={},
            todays_trades=1,
            execution_messages=execution_messages or [],
        )

    def test_run_success_renders_feedback_and_clears_stale_backtest_summary(self):
        fake_st = _FakeStreamlit(run_clicked=True)
        log_messages = []

        with patch.object(trading_page, 'st', fake_st):
            with patch.object(trading_page, '_ensure_output_files'):
                with patch.object(trading_page, '_minimal_theme'):
                    with patch.object(trading_page, 'run_operator_action', return_value=self._result(execution_messages=[('success', '1 trade executed')])):
                        with patch.object(trading_page, '_render_summary_cards'):
                            with patch.object(trading_page, '_render_operator_panels'):
                                with patch.object(trading_page, '_append_text_log', side_effect=lambda path, message: log_messages.append(message)):
                                    trading_page.main()

        self.assertNotIn('backtest_summary', fake_st.session_state)
        self.assertIn('1 trade executed', fake_st.successes)
        self.assertEqual(fake_st.errors, [])
        self.assertTrue(any('EXECUTION completed' in message for message in log_messages))
        self.assertGreaterEqual(len(fake_st.dataframes), 3)

    def test_backtest_success_persists_backtest_summary(self):
        fake_st = _FakeStreamlit(backtest_clicked=True)
        result = self._result(status='Backtest completed', backtest_summary={'total_trades': 4}, execution_messages=[])

        with patch.object(trading_page, 'st', fake_st):
            with patch.object(trading_page, '_ensure_output_files'):
                with patch.object(trading_page, '_minimal_theme'):
                    with patch.object(trading_page, 'run_operator_action', return_value=result):
                        with patch.object(trading_page, '_render_summary_cards'):
                            with patch.object(trading_page, '_render_operator_panels'):
                                with patch.object(trading_page, '_append_text_log'):
                                    trading_page.main()

        self.assertEqual(fake_st.session_state['backtest_summary'], {'total_trades': 4})
        self.assertEqual(fake_st.errors, [])

    def test_safe_failure_payload_is_rendered_as_error_instead_of_success(self):
        fake_st = _FakeStreamlit(run_clicked=True)
        log_messages = []
        result = self._result(status='Run failed: bad candle payload', execution_messages=[('error', 'bad candle payload')])

        with patch.object(trading_page, 'st', fake_st):
            with patch.object(trading_page, '_ensure_output_files'):
                with patch.object(trading_page, '_minimal_theme'):
                    with patch.object(trading_page, 'run_operator_action', return_value=result):
                        with patch.object(trading_page, '_render_summary_cards'):
                            with patch.object(trading_page, '_render_operator_panels'):
                                with patch.object(trading_page, '_append_text_log', side_effect=lambda path, message: log_messages.append(message)):
                                    trading_page.main()

        self.assertNotIn('backtest_summary', fake_st.session_state)
        self.assertIn('bad candle payload', fake_st.errors)
        self.assertIn('Run failed: bad candle payload', fake_st.errors)
        self.assertFalse(any('EXECUTION completed' in message for message in log_messages))
        self.assertIn('Run failed: bad candle payload', log_messages)


if __name__ == '__main__':
    unittest.main()

