import tempfile
import textwrap
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from src.dhan_api import load_security_map
from src.live_ohlcv import (
    _to_rows,
    _validate_dhan_ohlcv_rows,
    build_candle_cache_path,
    fetch_live_ohlcv,
    fetch_dhan_ohlcv,
    normalize_dhan_live_payload,
    read_candle_cache,
    write_candle_cache,
)


CSV_TEXT = textwrap.dedent(
    """\
    security_id,exchange,segment,exchange_segment,instrument_type,symbol_name,display_name,underlying_symbol,underlying_security_id,expiry_date,strike_price,option_type,lot_size
    1001,NSE,E,IDX_I,INDEX,NIFTY,NIFTY,NIFTY,, , , ,1
    1002,NSE,E,NSE_EQ,EQUITY,RELIANCE,RELIANCE,RELIANCE,, , , ,1
    """
)


class FakeDhanClient:
    def __init__(self):
        self.daily_calls = []
        self.intraday_calls = []

    def get_historical_data(self, **kwargs):
        self.daily_calls.append(kwargs)
        return {
            'open': [100.0, 102.0, 101.0],
            'high': [103.0, 104.0, 105.0],
            'low': [99.0, 100.0, 100.5],
            'close': [102.0, 101.0, 104.0],
            'volume': [1000, 1200, 1400],
            'timestamp': [1710979200, 1711065600, 1711152000],
            'open_interest': [0, 0, 0],
        }

    def get_intraday_data(self, **kwargs):
        self.intraday_calls.append(kwargs)
        return {
            'open': [100.0, 101.0, 102.0, 103.0],
            'high': [101.0, 102.0, 103.0, 104.0],
            'low': [99.5, 100.5, 101.5, 102.5],
            'close': [100.5, 101.5, 102.5, 103.5],
            'volume': [100, 110, 120, 130],
            'timestamp': [1710926100, 1710926400, 1710927000, 1710927300],
            'open_interest': [10, 11, 12, 13],
        }


class TestLiveOhlcv(unittest.TestCase):
    def _load_security_map(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        path = Path(tmpdir.name) / 'security_map.csv'
        path.write_text(CSV_TEXT, encoding='utf-8')
        return load_security_map(path)

    def test_to_rows(self):
        payload = {
            'chart': {
                'result': [
                    {
                        'timestamp': [1700000000, 1700000300],
                        'indicators': {
                            'quote': [
                                {
                                    'open': [100.1, 101.2],
                                    'high': [102.0, 102.4],
                                    'low': [99.7, 100.9],
                                    'close': [101.5, 102.1],
                                    'volume': [10000, 12000],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        rows = _to_rows(payload)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['open'], 100.1)
        self.assertEqual(rows[1]['volume'], 12000)
        self.assertEqual(rows[0]['provider'], 'YAHOO')

    def test_normalize_dhan_live_payload_preserves_schema(self):
        rows = normalize_dhan_live_payload(
            {
                'data': {
                    'timestamp': '2026-03-24 09:15:00',
                    'open': 100.0,
                    'high': 101.0,
                    'low': 99.5,
                    'close': 100.5,
                    'volume': 500,
                    'oi': 25,
                }
            },
            symbol='NIFTY',
            interval='1m',
            exchange_segment='IDX_I',
            security_id='1001',
            instrument='INDEX',
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['provider'], 'DHAN')
        self.assertEqual(rows[0]['source'], 'DHAN_LIVE_FEED')
        self.assertEqual(rows[0]['symbol'], 'NIFTY')
        self.assertEqual(rows[0]['open_interest'], 25)

    def test_validate_dhan_ohlcv_rows_rejects_missing_timestamp(self):
        payload = {
            'open': [100.0],
            'high': [101.0],
            'low': [99.5],
            'close': [100.5],
            'volume': [500],
            'timestamp': [None],
        }
        with self.assertRaisesRegex(ValueError, 'missing or invalid timestamp'):
            _validate_dhan_ohlcv_rows(payload, symbol='NIFTY', interval='1m', source='DHAN_HISTORICAL')

    def test_validate_dhan_ohlcv_rows_rejects_invalid_ohlc(self):
        payload = {
            'open': [100.0],
            'high': [99.0],
            'low': [98.0],
            'close': [100.5],
            'volume': [500],
            'timestamp': [1710926100],
        }
        with self.assertRaisesRegex(ValueError, 'invalid OHLC range'):
            _validate_dhan_ohlcv_rows(payload, symbol='NIFTY', interval='1m', source='DHAN_HISTORICAL')

    def test_validate_dhan_ohlcv_rows_rejects_duplicate_timestamp(self):
        payload = {
            'open': [100.0, 101.0],
            'high': [101.0, 102.0],
            'low': [99.5, 100.5],
            'close': [100.5, 101.5],
            'volume': [500, 600],
            'timestamp': [1710926100, 1710926100],
        }
        with self.assertRaisesRegex(ValueError, 'duplicate candle'):
            _validate_dhan_ohlcv_rows(payload, symbol='NIFTY', interval='1m', source='DHAN_HISTORICAL')

    def test_cache_round_trip(self):
        with tempfile.TemporaryDirectory() as td:
            cache_path = build_candle_cache_path(
                provider='DHAN',
                symbol='NIFTY',
                interval='5m',
                start_dt=datetime(2026, 3, 20, tzinfo=UTC),
                end_dt=datetime(2026, 3, 24, tzinfo=UTC),
                cache_dir=Path(td),
            )
            rows = [
                {
                    'timestamp': '2026-03-24 09:15:00',
                    'open': 100.0,
                    'high': 101.0,
                    'low': 99.0,
                    'close': 100.5,
                    'volume': 500,
                    'price': 100.5,
                    'interval': '5m',
                    'provider': 'DHAN',
                    'symbol': 'NIFTY',
                    'source': 'DHAN_HISTORICAL',
                    'exchange_segment': 'IDX_I',
                    'security_id': '1001',
                    'instrument': 'INDEX',
                    'open_interest': 0,
                    'is_closed': True,
                }
            ]
            write_candle_cache(cache_path, rows)
            loaded = read_candle_cache(cache_path)
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]['provider'], 'DHAN')
            self.assertEqual(loaded[0]['symbol'], 'NIFTY')

    def test_fetch_dhan_ohlcv_uses_cache(self):
        security_map = self._load_security_map()
        fake_client = FakeDhanClient()
        with tempfile.TemporaryDirectory() as td:
            first = fetch_dhan_ohlcv(
                'NIFTY',
                '5m',
                '1d',
                security_map=security_map,
                broker_client=fake_client,
                cache_dir=Path(td),
                force_refresh=True,
            )
            second = fetch_dhan_ohlcv(
                'NIFTY',
                '5m',
                '1d',
                security_map=security_map,
                broker_client=fake_client,
                cache_dir=Path(td),
                use_cache=True,
            )
            self.assertTrue(first)
            self.assertEqual(first, second)
            self.assertEqual(len(fake_client.intraday_calls), 1)
            self.assertEqual(first[0]['provider'], 'DHAN')
            self.assertEqual(first[0]['source'], 'DHAN_HISTORICAL')

    def test_fetch_dhan_ohlcv_rejects_invalid_payload_before_strategy_use(self):
        security_map = self._load_security_map()

        class BrokenDhanClient(FakeDhanClient):
            def get_intraday_data(self, **kwargs):
                return {
                    'open': [100.0, 101.0],
                    'high': [101.0, 102.0],
                    'low': [99.5, 100.5],
                    'close': [100.5, 101.5],
                    'volume': [100, 110],
                    'timestamp': [1710926100, 1710926100],
                    'open_interest': [10, 11],
                }

        with tempfile.TemporaryDirectory() as td:
            with self.assertRaisesRegex(ValueError, 'duplicate candle'):
                fetch_dhan_ohlcv(
                    'NIFTY',
                    '5m',
                    '1d',
                    security_map=security_map,
                    broker_client=BrokenDhanClient(),
                    cache_dir=Path(td),
                    force_refresh=True,
                )

    def test_fetch_live_ohlcv_auto_prefers_yahoo_before_dhan(self):
        yahoo_rows = [{'timestamp': '2026-03-24 09:15:00', 'provider': 'YAHOO'}]
        with patch('src.live_ohlcv._fetch_yfinance_ohlcv', return_value=yahoo_rows) as yahoo_mock:
            with patch('src.live_ohlcv.fetch_dhan_ohlcv') as dhan_mock:
                rows = fetch_live_ohlcv('NIFTY', '5m', '1d', provider='AUTO')
        self.assertEqual(rows, yahoo_rows)
        yahoo_mock.assert_called_once()
        dhan_mock.assert_not_called()

    def test_fetch_live_ohlcv_auto_falls_back_to_dhan_when_yahoo_fails(self):
        dhan_rows = [{'timestamp': '2026-03-24 09:15:00', 'provider': 'DHAN'}]
        with patch('src.live_ohlcv._fetch_yfinance_ohlcv', side_effect=RuntimeError('timeout')) as yahoo_mock:
            with patch('src.live_ohlcv.fetch_dhan_ohlcv', return_value=dhan_rows) as dhan_mock:
                rows = fetch_live_ohlcv('NIFTY', '5m', '1d', provider='AUTO')
        self.assertEqual(rows, dhan_rows)
        yahoo_mock.assert_called_once()
        dhan_mock.assert_called_once()


if __name__ == '__main__':
    unittest.main()
