import sys
import types
import unittest
from unittest.mock import patch

sys.modules.setdefault('yfinance', types.SimpleNamespace())

from src.trading_runtime_service import _attach_option_metrics


class TestTradingRuntimeOptionMetrics(unittest.TestCase):
    @patch('src.trading_runtime_service.fetch_option_chain')
    def test_attach_option_metrics_adds_chain_metrics_and_decay_flags(self, mock_fetch):
        mock_fetch.return_value = {
            'records': {
                'data': [
                    {
                        'strikePrice': 22000,
                        'expiryDate': '2026-03-30',
                        'underlyingValue': 22010,
                        'CE': {
                            'lastPrice': 120.0,
                            'openInterest': 1000,
                            'totalTradedVolume': 500,
                            'impliedVolatility': 14.5,
                        },
                    }
                ]
            }
        }
        rows = [
            {
                'side': 'BUY',
                'entry_price': 22005.0,
                'spot_price': 22010.0,
                'strike_price': 22000,
                'option_type': 'CE',
                'option_strike': '22000CE',
            }
        ]

        enriched = _attach_option_metrics(rows, symbol='NIFTY', fetch_option_metrics=True)

        self.assertEqual(enriched[0]['option_metrics_status'], 'ATTACHED')
        self.assertEqual(enriched[0]['option_ltp'], 120.0)
        self.assertIn(enriched[0]['theta_decay_risk'], {'HIGH', 'MEDIUM', 'LOW'})
        self.assertIn(enriched[0]['gamma_risk'], {'HIGH', 'MEDIUM', 'LOW'})
        self.assertIn('option_decay_summary', enriched[0])

    @patch('src.trading_runtime_service.fetch_option_chain', side_effect=RuntimeError('chain unavailable'))
    def test_attach_option_metrics_falls_back_cleanly_when_chain_fails(self, _mock_fetch):
        rows = [
            {
                'side': 'BUY',
                'entry_price': 22005.0,
                'spot_price': 22010.0,
                'strike_price': 22000,
                'option_type': 'CE',
                'option_strike': '22000CE',
                'option_expiry': '2026-03-30',
            }
        ]

        enriched = _attach_option_metrics(rows, symbol='NIFTY', fetch_option_metrics=True)

        self.assertEqual(enriched[0]['option_metrics_status'], 'UNAVAILABLE')
        self.assertIn('theta_decay_risk', enriched[0])
        self.assertIn('gamma_risk', enriched[0])


if __name__ == '__main__':
    unittest.main()
