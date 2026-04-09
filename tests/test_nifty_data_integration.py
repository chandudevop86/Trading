import unittest
from unittest.mock import patch

import pandas as pd

from src.nifty_data_integration import NiftyDataValidationError, fetch_nifty_data_bundle
from src.market_data_service import fetch_ohlcv_data as fetch_market_data_frame
from src.trading_runtime_service import fetch_ohlcv_data as fetch_runtime_data_frame


class TestNiftyDataIntegration(unittest.TestCase):
    def test_fetch_bundle_falls_back_when_first_provider_fails_strict_validation(self):
        yahoo_rows = [
            {
                'timestamp': '2026-03-20 09:15:00',
                'open': 100.0,
                'high': 101.0,
                'low': 99.5,
                'close': 100.5,
                'volume': 0,
            },
            {
                'timestamp': '2026-03-20 09:20:00',
                'open': 100.5,
                'high': 101.2,
                'low': 100.0,
                'close': 101.0,
                'volume': 0,
            },
        ]
        dhan_rows = []
        for minute in range(20):
            dhan_rows.append(
                {
                    'timestamp': f'2026-03-20 09:{15 + minute:02d}:00',
                    'open': 100.0 + minute,
                    'high': 100.5 + minute,
                    'low': 99.5 + minute,
                    'close': 100.2 + minute,
                    'volume': 1000 + minute,
                }
            )

        def fake_fetch(symbol, interval, period, **kwargs):
            provider = kwargs.get('provider')
            return yahoo_rows if provider == 'YAHOO' else dhan_rows

        with patch('src.nifty_data_integration.fetch_live_ohlcv', side_effect=fake_fetch):
            bundle = fetch_nifty_data_bundle('NIFTY', interval='1m', period='1d')

        self.assertEqual(bundle.provider, 'DHAN')
        self.assertEqual(bundle.provider_attempts[0].provider, 'YAHOO')
        self.assertFalse(bundle.provider_attempts[0].passed)
        self.assertIn('volume is entirely zero', bundle.provider_attempts[0].reason)
        self.assertTrue(bundle.validation_report['passed'])
        self.assertIn('vwap', bundle.frame.columns)
        self.assertEqual(bundle.symbol, 'NIFTY')

    def test_fetch_bundle_rejects_offhours_intraday_rows(self):
        rows = [
            {
                'timestamp': '2026-03-20 08:00:00',
                'open': 100.0,
                'high': 101.0,
                'low': 99.0,
                'close': 100.5,
                'volume': 1000,
            }
            for _ in range(20)
        ]
        for index, row in enumerate(rows):
            row['timestamp'] = f'2026-03-20 08:{index:02d}:00'

        with patch('src.nifty_data_integration.fetch_live_ohlcv', return_value=rows):
            with self.assertRaisesRegex(NiftyDataValidationError, 'offhours candles detected'):
                fetch_nifty_data_bundle('NIFTY', interval='1m', period='1d', provider='YAHOO')

    def test_fetch_bundle_rejects_too_many_interval_breaks(self):
        rows = []
        minutes = [15, 16, 25, 26, 40, 41, 55, 56, 57, 58, 59, 60, 75, 76, 90, 91, 105, 106, 120, 121]
        for minute in minutes:
            hour = 9 + (minute // 60)
            mm = minute % 60
            rows.append(
                {
                    'timestamp': f'2026-03-20 {hour:02d}:{mm:02d}:00',
                    'open': 100.0 + minute,
                    'high': 101.0 + minute,
                    'low': 99.5 + minute,
                    'close': 100.5 + minute,
                    'volume': 1000 + minute,
                }
            )

        with patch('src.nifty_data_integration.fetch_live_ohlcv', return_value=rows):
            with self.assertRaisesRegex(NiftyDataValidationError, 'interval integrity too weak'):
                fetch_nifty_data_bundle('NIFTY', interval='1m', period='1d', provider='YAHOO')

    def test_fetch_bundle_rejects_abnormal_price_jump(self):
        rows = []
        for minute in range(20):
            close = 100.0 + minute
            if minute == 10:
                close = 140.0
            rows.append(
                {
                    'timestamp': f'2026-03-20 09:{15 + minute:02d}:00',
                    'open': close - 0.2,
                    'high': close + 0.3,
                    'low': close - 0.5,
                    'close': close,
                    'volume': 1000 + minute,
                }
            )

        with patch('src.nifty_data_integration.fetch_live_ohlcv', return_value=rows):
            with self.assertRaisesRegex(NiftyDataValidationError, 'abnormal candle-to-candle jumps detected'):
                fetch_nifty_data_bundle('NIFTY', interval='1m', period='1d', provider='YAHOO')

    def test_fetch_bundle_exposes_deep_validation_metrics(self):
        rows = []
        for minute in range(20):
            rows.append(
                {
                    'timestamp': f'2026-03-20 09:{15 + minute:02d}:00',
                    'open': 100.0 + minute,
                    'high': 100.5 + minute,
                    'low': 99.5 + minute,
                    'close': 100.2 + minute,
                    'volume': 1000 + minute,
                }
            )

        with patch('src.nifty_data_integration.fetch_live_ohlcv', return_value=rows):
            bundle = fetch_nifty_data_bundle('NIFTY', interval='1m', period='1d', provider='YAHOO')

        self.assertEqual(bundle.validation_report['gap_count'], 0)
        self.assertEqual(bundle.validation_report['invalid_interval_count'], 0)
        self.assertEqual(bundle.validation_report['abnormal_return_count'], 0)
        self.assertEqual(bundle.validation_report['critical_nan_columns'], [])
        self.assertTrue(bundle.validation_report['passed'])

    def test_fetch_bundle_skips_freshness_hard_fail_outside_market_session(self):
        rows = []
        for minute in range(20):
            rows.append(
                {
                    'timestamp': f'2026-03-20 09:{15 + minute:02d}:00',
                    'open': 100.0 + minute,
                    'high': 100.5 + minute,
                    'low': 99.5 + minute,
                    'close': 100.2 + minute,
                    'volume': 1000 + minute,
                }
            )

        with patch('src.nifty_data_integration.fetch_live_ohlcv', return_value=rows), patch('src.nifty_data_integration._is_market_session_now', return_value=False):
            bundle = fetch_nifty_data_bundle(
                'NIFTY',
                interval='1m',
                period='1d',
                provider='YAHOO',
                require_freshness=True,
                max_staleness_minutes=3,
            )

        self.assertTrue(bundle.validation_report['freshness_checked'])
        self.assertFalse(bundle.validation_report['freshness_enforced'])
        self.assertTrue(bundle.validation_report['freshness_passed'])
        self.assertIn('freshness_not_enforced_outside_market_session', bundle.validation_report['warnings'])

    def test_fetch_bundle_rejects_stale_intraday_feed_when_freshness_required(self):
        rows = []
        for minute in range(20):
            rows.append(
                {
                    'timestamp': f'2026-03-20 09:{15 + minute:02d}:00',
                    'open': 100.0 + minute,
                    'high': 100.5 + minute,
                    'low': 99.5 + minute,
                    'close': 100.2 + minute,
                    'volume': 1000 + minute,
                }
            )

        with patch('src.nifty_data_integration.fetch_live_ohlcv', return_value=rows), patch('src.nifty_data_integration._is_market_session_now', return_value=True):
            with self.assertRaisesRegex(NiftyDataValidationError, 'stale intraday feed'):
                fetch_nifty_data_bundle(
                    'NIFTY',
                    interval='1m',
                    period='1d',
                    provider='YAHOO',
                    require_freshness=True,
                    max_staleness_minutes=3,
                )

    def test_market_and_runtime_fetchers_use_strict_integration_frame(self):
        frame = pd.DataFrame(
            [
                {
                    'timestamp': pd.Timestamp('2026-03-20 09:15:00'),
                    'open': 100.0,
                    'high': 101.0,
                    'low': 99.5,
                    'close': 100.5,
                    'volume': 1000.0,
                    'vwap': 100.4,
                }
            ]
        )
        with patch('src.market_data_service.fetch_nifty_ohlcv_frame', return_value=frame) as market_mock:
            returned = fetch_market_data_frame('NIFTY', interval='5m', period='5d')
        market_mock.assert_called_once()
        self.assertEqual(returned.iloc[0]['close'], 100.5)

        with patch('src.trading_runtime_service.fetch_nifty_ohlcv_frame', return_value=frame) as runtime_mock:
            returned = fetch_runtime_data_frame('NIFTY', interval='5m', period='5d')
        runtime_mock.assert_called_once()
        self.assertEqual(returned.iloc[0]['vwap'], 100.4)


if __name__ == '__main__':
    unittest.main()



