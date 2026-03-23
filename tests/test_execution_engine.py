import os
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from src.execution_engine import (
    build_analysis_queue,
    build_execution_candidates,
    default_quantity_for_symbol,
    execute_live_trades,
    execute_paper_trades,
    execution_result_summary,
    filter_unlogged_candidates,
    live_kill_switch_enabled,
    live_trading_unlock_status,
    load_open_trades,
    make_trade_id,
    make_trade_key,
    manual_close_paper_trades,
    normalize_order_quantity,
    reconcile_live_positions,
    reconcile_live_trades,
)


class _StubBrokerClient:
    client_id = 'test-client'

    def __init__(self):
        self.calls = 0
        self.fetch_calls = 0

    def place_order(self, request):
        self.calls += 1
        return {'orderId': 'ORD123', 'orderStatus': 'TRANSIT', 'message': 'accepted', 'echo': request.to_payload()}

    def get_order_by_id(self, order_id):
        self.fetch_calls += 1
        return {'orderId': order_id, 'orderStatus': 'TRADED', 'message': 'filled'}

    def get_positions(self):
        return [
            {'tradingSymbol': 'NIFTY', 'netQty': 65},
            {'tradingSymbol': 'BANKNIFTY', 'netQty': -50},
        ]


class TestExecutionEngine(unittest.TestCase):
    def test_build_indicator_candidate(self):
        rows = [{'timestamp': '2026-03-06 10:00:00', 'market_signal': 'BULLISH_TREND', 'close': 22350.0}]
        c = build_execution_candidates('Indicator (RSI/ADX/MACD+VWAP)', rows, 'NIFTY')
        self.assertEqual(len(c), 1)
        self.assertEqual(c[0]['side'], 'BUY')
        self.assertEqual(c[0]['quantity'], 65)
        self.assertIn('share_price', c[0])
        self.assertIn('strike_price', c[0])

    def test_default_quantity_for_symbol(self):
        self.assertEqual(default_quantity_for_symbol('NIFTY'), 65)
        self.assertEqual(default_quantity_for_symbol('BANKNIFTY'), 1)

    def test_build_analysis_queue_filters_actionable_candidates(self):
        analyzed = build_analysis_queue(
            [
                {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:00:00', 'side': 'BUY', 'price': 100},
                {'strategy': 'INDICATOR', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:05:00', 'side': 'HOLD', 'price': 101},
            ],
            analyzed_at_utc='2026-03-06 10:10:00',
        )
        self.assertEqual(len(analyzed), 1)
        self.assertEqual(analyzed[0]['analysis_status'], 'ANALYZED')
        self.assertEqual(analyzed[0]['execution_ready'], 'YES')
        self.assertEqual(analyzed[0]['analyzed_at_utc'], '2026-03-06 10:10:00')

    def test_build_execution_candidates_preserves_trade_labels(self):
        rows = [
            {
                'strategy': 'MTF_5M',
                'entry_time': '2026-03-05 10:50:00',
                'side': 'BUY',
                'entry_price': 107.3,
                'stop_loss': 105.1,
                'trailing_stop_loss': 105.1,
                'target_price': 109.5,
                'quantity': 65,
                'trade_no': 2,
                'trade_label': 'Trade 2',
                'option_type': 'CE',
                'strike_price': 23450,
            }
        ]

        candidates = build_execution_candidates('MTF 5m', rows, 'NIFTY')

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]['trade_no'], 2)
        self.assertEqual(candidates[0]['trade_label'], 'Trade 2')

    def test_make_trade_identity_is_stable(self):
        candidate = {
            'strategy': 'BREAKOUT',
            'symbol': 'NIFTY',
            'signal_time': '2026-03-06 10:00:00',
            'side': 'BUY',
            'price': 100.0,
            'option_strike': '22550CE',
        }
        self.assertEqual(make_trade_id(candidate), make_trade_id(candidate))
        self.assertEqual(make_trade_key(candidate), make_trade_key(candidate))

    def test_execute_paper_trades_returns_structured_summary(self):
        candidates = [
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:00:00', 'side': 'BUY', 'price': 100, 'quantity': 65, 'reason': 'x'},
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:05:00', 'side': 'HOLD', 'price': 101, 'quantity': 65, 'reason': 'y'},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            result = execute_paper_trades(candidates, out)
            self.assertEqual(result.executed_count, 1)
            self.assertEqual(result.skipped_count, 1)
            self.assertEqual(result.error_count, 0)
            messages = execution_result_summary(result)
            self.assertTrue(any('1 trade executed' in message for _, message in messages))
            self.assertTrue(any('invalid side' in message for _, message in messages))
    def test_normalize_order_quantity_nifty(self):
        self.assertEqual(normalize_order_quantity('NIFTY', 10), 65)
        self.assertEqual(normalize_order_quantity('NIFTY', 129), 65)
        self.assertEqual(normalize_order_quantity('NIFTY', 130), 130)

    def test_live_kill_switch_enabled_helper(self):
        original = os.environ.get('LIVE_TRADING_KILL_SWITCH')
        try:
            os.environ['LIVE_TRADING_KILL_SWITCH'] = 'true'
            self.assertTrue(live_kill_switch_enabled())
            os.environ['LIVE_TRADING_KILL_SWITCH'] = '0'
            self.assertFalse(live_kill_switch_enabled())
        finally:
            if original is None:
                os.environ.pop('LIVE_TRADING_KILL_SWITCH', None)
            else:
                os.environ['LIVE_TRADING_KILL_SWITCH'] = original

    def test_execute_paper_trades(self):
        candidates = [
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:00:00', 'side': 'BUY', 'price': 100, 'quantity': 10, 'reason': 'x'},
            {'strategy': 'INDICATOR', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:05:00', 'side': 'HOLD', 'price': 101, 'quantity': 1, 'reason': 'neutral'},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            done = execute_paper_trades(candidates, out)
            self.assertEqual(len(done), 1)
            self.assertTrue(out.exists())
            self.assertEqual(done[0]['quantity'], 65)
            self.assertIn('share_price', done[0])
            self.assertIn('strike_price', done[0])

    def test_execute_paper_trades_deduplicates(self):
        candidates = [
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:00:00', 'side': 'BUY', 'price': 100, 'quantity': 10, 'reason': 'x'}
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            first = execute_paper_trades(candidates, out, deduplicate=True)
            second = execute_paper_trades(candidates, out, deduplicate=True)
            self.assertEqual(len(first), 1)
            self.assertEqual(len(second), 0)

    def test_filter_unlogged_candidates_skips_already_logged_rows(self):
        candidates = [
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:00:00', 'side': 'BUY', 'price': 100, 'quantity': 65, 'reason': 'x'},
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:05:00', 'side': 'SELL', 'price': 99, 'quantity': 65, 'reason': 'y'},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            execute_paper_trades([candidates[0]], out, deduplicate=True)
            fresh, skipped = filter_unlogged_candidates(candidates, out)
            self.assertEqual(len(fresh), 1)
            self.assertEqual(fresh[0]['signal_time'], '2026-03-06 10:05:00')
            self.assertEqual(len(skipped), 1)
            self.assertEqual(skipped[0]['signal_time'], '2026-03-06 10:00:00')

    def test_manual_close_paper_trades_closes_selected_trade(self):
        candidates = [
            {'strategy': 'MANUAL', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:00:00', 'side': 'BUY', 'price': 100, 'quantity': 65, 'reason': 'manual'}
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            result = execute_paper_trades(candidates, out)
            open_rows = load_open_trades(out, 'PAPER')
            self.assertEqual(len(open_rows), 1)
            closed = manual_close_paper_trades(out, [open_rows[0]['trade_id']], exit_price=102.0)
            self.assertEqual(len(closed), 1)
            self.assertEqual(closed[0]['execution_status'], 'CLOSED')
            self.assertEqual(closed[0]['trade_status'], 'CLOSED')
            self.assertEqual(closed[0]['position_status'], 'CLOSED')
            remaining_open = load_open_trades(out, 'PAPER')
            self.assertEqual(len(remaining_open), 0)
    def test_execute_paper_trades_blocks_after_daily_trade_limit(self):
        candidates = [
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 10:00:00', 'side': 'BUY', 'price': 100, 'quantity': 65, 'reason': 'x'},
            {'strategy': 'BTST', 'symbol': 'NIFTY', 'signal_time': '2026-03-06 11:00:00', 'side': 'SELL', 'price': 99, 'quantity': 65, 'reason': 'y'},
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'executed.csv'
            rows = execute_paper_trades(candidates, out, max_trades_per_day=1)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]['execution_status'], 'EXECUTED')
            self.assertEqual(rows[1]['execution_status'], 'BLOCKED')
            self.assertIn('max trades/day=1', rows[1]['risk_limit_reason'])

    def test_execute_live_trades_blocks_after_daily_loss_limit(self):
        candidates = [
            {
                'strategy': 'BREAKOUT',
                'symbol': 'NIFTY',
                'signal_time': '2026-03-06 10:00:00',
                'side': 'BUY',
                'price': 100.0,
                'quantity': 65,
                'reason': 'x',
                'security_id': '12345',
                'pnl': -1500.0,
            }
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'live.csv'
            out.write_text(
                'strategy,symbol,signal_time,side,price,quantity,execution_type,execution_status,executed_at_utc,pnl\n'
                'BREAKOUT,NIFTY,2026-03-06 09:30:00,SELL,100,65,LIVE,SENT,2026-03-06 09:30:00,-1500\n',
                encoding='utf-8',
            )
            rows = execute_live_trades(
                candidates,
                out,
                broker_client=_StubBrokerClient(),
                broker_name='DHAN',
                security_map={},
                max_daily_loss=1000.0,
            )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['execution_status'], 'BLOCKED')
            self.assertEqual(rows[0]['broker_status'], 'RISK_LIMIT')

    def test_execute_live_trades_blocks_when_kill_switch_enabled(self):
        original = os.environ.get('LIVE_TRADING_KILL_SWITCH')
        try:
            os.environ['LIVE_TRADING_KILL_SWITCH'] = 'true'
            broker = _StubBrokerClient()
            candidates = [
                {
                    'strategy': 'BREAKOUT',
                    'symbol': 'NIFTY',
                    'signal_time': '2026-03-06 10:00:00',
                    'side': 'BUY',
                    'price': 100.0,
                    'quantity': 65,
                    'reason': 'x',
                    'security_id': '12345',
                }
            ]
            with tempfile.TemporaryDirectory() as td:
                out = Path(td) / 'live.csv'
                rows = execute_live_trades(
                    candidates,
                    out,
                    broker_client=broker,
                    broker_name='DHAN',
                    security_map={},
                )
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['execution_status'], 'BLOCKED')
            self.assertEqual(rows[0]['broker_status'], 'KILL_SWITCH')
            self.assertEqual(broker.calls, 0)
        finally:
            if original is None:
                os.environ.pop('LIVE_TRADING_KILL_SWITCH', None)
            else:
                os.environ['LIVE_TRADING_KILL_SWITCH'] = original

    def test_reconcile_live_trades_updates_filled_orders(self):
        broker = _StubBrokerClient()
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'live.csv'
            out.write_text(
                'strategy,symbol,signal_time,side,price,quantity,execution_type,execution_status,executed_at_utc,broker_order_id,broker_status\n'
                'BREAKOUT,NIFTY,2026-03-06 09:30:00,BUY,100,65,LIVE,SENT,2026-03-06 09:30:00,ORD123,TRANSIT\n',
                encoding='utf-8',
            )
            rows = reconcile_live_trades(out, broker_client=broker, broker_name='DHAN')
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['execution_status'], 'FILLED')
            self.assertEqual(rows[0]['broker_status'], 'TRADED')
            self.assertEqual(rows[0]['reconciliation_status'], 'RECONCILED')
            self.assertEqual(broker.fetch_calls, 1)
            saved = out.read_text(encoding='utf-8')
            self.assertIn('FILLED', saved)
            self.assertIn('RECONCILED', saved)

    def test_reconcile_live_positions_compares_expected_vs_broker(self):
        broker = _StubBrokerClient()
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'live.csv'
            out.write_text(
                'strategy,symbol,signal_time,side,quantity,execution_type,execution_status,broker_order_id\n'
                'BREAKOUT,NIFTY,2026-03-06 09:30:00,BUY,65,LIVE,FILLED,ORD123\n'
                'BREAKOUT,BANKNIFTY,2026-03-06 10:00:00,SELL,25,LIVE,FILLED,ORD124\n',
                encoding='utf-8',
            )
            rows = reconcile_live_positions(out, broker_client=broker, broker_name='DHAN')
            self.assertEqual(len(rows), 2)
            by_symbol = {row['symbol']: row for row in rows}
            self.assertEqual(by_symbol['NIFTY']['expected_net_qty'], 65)
            self.assertEqual(by_symbol['NIFTY']['broker_net_qty'], 65)
            self.assertEqual(by_symbol['NIFTY']['position_match'], 'YES')
            self.assertEqual(by_symbol['BANKNIFTY']['expected_net_qty'], -25)
            self.assertEqual(by_symbol['BANKNIFTY']['broker_net_qty'], -50)
            self.assertEqual(by_symbol['BANKNIFTY']['position_match'], 'NO')
            self.assertEqual(by_symbol['BANKNIFTY']['qty_delta'], -25)

    def test_live_trading_unlock_after_30_days(self):
        candidates = [
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-01-01 10:00:00', 'side': 'BUY', 'price': 100, 'quantity': 65, 'reason': 'x'}
        ]

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / 'dhan_paper.csv'
            execute_paper_trades(candidates, out)

            lines = out.read_text(encoding='utf-8').splitlines()
            header = lines[0]
            row = lines[1].split(',')
            cols = header.split(',')
            idx = cols.index('executed_at_utc')
            row[idx] = '2026-01-01 00:00:00'
            out.write_text('\n'.join([header, ','.join(row)]) + '\n', encoding='utf-8')

            unlocked, days, unlock_date = live_trading_unlock_status(
                out,
                min_days=30,
                now_utc=datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC),
            )
            self.assertTrue(unlocked)
            self.assertGreaterEqual(days, 30)
            self.assertIn('2026-01-31', unlock_date)


if __name__ == '__main__':
    unittest.main()
