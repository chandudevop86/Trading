import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.dhan_streams import CandleAggregator, normalize_dhan_market_feed_payload, normalize_dhan_order_update_payload
from src.execution_engine import apply_live_order_updates_to_log
from src.live_trading_runtime import DhanLiveTradingRuntime, LiveTradingConfig


class TestDhanStreams(unittest.TestCase):
    def test_market_feed_payload_normalizes_and_closes_candle(self):
        payload_one = {
            'data': {
                'timestamp': '2026-03-24 09:15:05',
                'ltp': 100.5,
                'volume': 100,
                'oi': 10,
            }
        }
        payload_two = {
            'data': {
                'timestamp': '2026-03-24 09:16:02',
                'ltp': 101.0,
                'volume': 120,
                'oi': 11,
            }
        }
        events_one = normalize_dhan_market_feed_payload(payload_one, symbol='NIFTY', interval='1m', exchange_segment='IDX_I', security_id='1001', instrument='INDEX')
        self.assertEqual(len(events_one), 1)
        self.assertEqual(events_one[0].ltp, 100.5)

        aggregator = CandleAggregator(symbol='NIFTY', interval='1m', exchange_segment='IDX_I', security_id='1001', instrument='INDEX')
        self.assertEqual(aggregator.ingest(events_one[0]), [])
        events_two = normalize_dhan_market_feed_payload(payload_two, symbol='NIFTY', interval='1m', exchange_segment='IDX_I', security_id='1001', instrument='INDEX')
        closed = aggregator.ingest(events_two[0])
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0]['timestamp'], '2026-03-24 09:15:00')
        self.assertTrue(closed[0]['is_closed'])

    def test_order_update_payload_updates_live_log(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'live_log.csv'
            with path.open('w', newline='', encoding='utf-8') as handle:
                writer = csv.DictWriter(handle, fieldnames=['trade_id', 'execution_type', 'broker_order_id', 'correlation_id', 'execution_status', 'trade_status', 'position_status'])
                writer.writeheader()
                writer.writerow({
                    'trade_id': 'trade-1',
                    'execution_type': 'LIVE',
                    'broker_order_id': 'order-1',
                    'correlation_id': 'corr-1',
                    'execution_status': 'SENT',
                    'trade_status': 'PENDING_EXECUTION',
                    'position_status': '',
                })

            payload = {
                'data': {
                    'orderId': 'order-1',
                    'correlationId': 'corr-1',
                    'orderStatus': 'TRADED',
                    'filledQty': 50,
                    'quantity': 50,
                    'averagePrice': 101.25,
                    'timestamp': '2026-03-24 09:17:00',
                }
            }
            events = normalize_dhan_order_update_payload(payload)
            changed = apply_live_order_updates_to_log(path, events)
            self.assertEqual(len(changed), 1)
            self.assertEqual(changed[0]['execution_status'], 'FILLED')
            self.assertEqual(changed[0]['trade_status'], 'OPEN')

    def test_runtime_emits_candidates_once_per_trade(self):
        runtime = DhanLiveTradingRuntime(
            LiveTradingConfig(
                strategy='Breakout',
                symbol='NIFTY',
                interval='1m',
                execution_symbol='NIFTY',
                strategy_label='Breakout (Live)',
            )
        )
        first_payload = {'data': {'timestamp': '2026-03-24 09:15:01', 'ltp': 100.0, 'volume': 100, 'oi': 10}}
        second_payload = {'data': {'timestamp': '2026-03-24 09:16:01', 'ltp': 101.0, 'volume': 120, 'oi': 11}}
        third_payload = {'data': {'timestamp': '2026-03-24 09:17:01', 'ltp': 102.0, 'volume': 130, 'oi': 12}}

        with patch('src.live_trading_runtime.generate_strategy_rows', return_value=[{'side': 'BUY', 'entry_price': 101.0, 'timestamp': '2026-03-24 09:16:00', 'strategy': 'BREAKOUT'}]), patch('src.live_trading_runtime.build_execution_candidates', return_value=[{'trade_id': 'trade-1', 'side': 'BUY', 'symbol': 'NIFTY'}]):
            runtime.ingest_market_payload(first_payload)
            snap_one = runtime.ingest_market_payload(second_payload)
            snap_two = runtime.ingest_market_payload(third_payload)

        self.assertEqual(len(snap_one.execution_candidates), 1)
        self.assertEqual(snap_one.execution_candidates[0]['trade_id'], 'trade-1')
        self.assertEqual(snap_two.execution_candidates, [])


if __name__ == '__main__':
    unittest.main()