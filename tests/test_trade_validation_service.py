import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from src.execution_engine import execute_paper_trades
from src.preprocessing import prepare_trading_data
from src.reporting_service import paper_execution_summary
from src.trade_validation_service import (
    PaperReadinessConfig,
    build_trade_evaluation_summary,
    calculate_trade_metrics,
    evaluate_paper_readiness,
    parse_trade_timestamp,
)


class TestTradeValidationService(unittest.TestCase):
    def test_parse_trade_timestamp_normalizes_offset_aware_values(self):
        parsed = parse_trade_timestamp('2026-03-28T09:15:00+05:30')
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.strftime('%Y-%m-%d %H:%M:%S'), '2026-03-28 03:45:00')

    def test_prepare_trading_data_normalizes_ohlcv_schema(self):
        prepared = prepare_trading_data(
            [
                {'Date': '2026-03-28', 'Time': '09:20:00', 'Open': '100', 'High': '102', 'Low': '99', 'Close': '101', 'Vol': '1200'},
                {'timestamp': '2026-03-28 09:15:00', 'open': 99, 'high': 101, 'low': 98, 'close': 100, 'volume': 1000},
            ]
        )
        self.assertEqual(list(prepared.columns), ['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        self.assertEqual(str(prepared.iloc[0]['timestamp']), '2026-03-28 09:15:00')
        self.assertEqual(float(prepared.iloc[1]['volume']), 1200.0)

    def test_calculate_trade_metrics_returns_consistent_summary(self):
        summary = calculate_trade_metrics(
            [
                {'strategy': 'Breakout', 'symbol': 'NIFTY', 'execution_type': 'PAPER', 'side': 'BUY', 'signal_time': '2026-03-28 09:20:00', 'entry_time': '2026-03-28 09:20:00', 'exit_time': '2026-03-28 09:35:00', 'entry_price': 100.0, 'exit_price': 102.0, 'pnl': 120.0, 'execution_status': 'CLOSED'},
                {'strategy': 'Breakout', 'symbol': 'NIFTY', 'execution_type': 'PAPER', 'side': 'SELL', 'signal_time': '2026-03-28 09:40:00', 'entry_time': '2026-03-28 09:40:00', 'exit_time': '2026-03-28 09:50:00', 'entry_price': 101.0, 'exit_price': 102.0, 'pnl': -60.0, 'execution_status': 'CLOSED'},
                {'strategy': 'Breakout', 'symbol': 'NIFTY', 'execution_type': 'PAPER', 'side': 'BUY', 'signal_time': '2026-03-28 10:00:00', 'entry_time': '2026-03-28 10:00:00', 'price': 103.0, 'pnl': 0.0, 'execution_status': 'EXECUTED'},
            ],
            strategy_name='BREAKOUT',
        )
        self.assertEqual(summary['total_trades'], 3)
        self.assertEqual(summary['closed_trades'], 2)
        self.assertEqual(summary['open_trades'], 1)
        self.assertEqual(summary['wins'], 1)
        self.assertEqual(summary['losses'], 1)
        self.assertEqual(summary['win_rate'], 50.0)
        self.assertEqual(summary['profit_factor'], 2.0)
        self.assertGreater(summary['max_drawdown'], 0.0)
        self.assertEqual(summary['drawdown_proven'], 'YES')

    def test_evaluate_paper_readiness_blocks_weak_sample_in_plain_english(self):
        readiness = evaluate_paper_readiness(
            {
                'total_trades': 5,
                'expectancy_per_trade': -10.0,
                'profit_factor': 0.8,
                'max_drawdown_pct': 15.0,
                'execution_error_count': 1,
                'duplicate_trade_count': 2,
                'invalid_trade_count': 1,
            },
            PaperReadinessConfig(min_trades=30, min_profit_factor=1.2, max_drawdown_pct=10.0),
        )
        self.assertEqual(readiness['paper_ready'], 'NO')
        self.assertIn('not enough paper trades yet', readiness['paper_readiness_blockers'])
        self.assertIn('expectancy is not positive yet', readiness['paper_readiness_summary'])
        self.assertIn('Stay in paper trading', readiness['paper_readiness_next_step'])

    def test_execute_paper_trades_enforces_invalid_and_daily_limit_constraints(self):
        candidates = [
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-28 09:20:00', 'side': 'BUY', 'price': 100.0, 'quantity': 65, 'reason': 'valid one'},
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-28 09:50:00', 'side': 'SELL', 'price': 101.0, 'quantity': 65, 'reason': 'valid two'},
            {'strategy': 'BREAKOUT', 'symbol': 'NIFTY', 'signal_time': '2026-03-28 09:35:00', 'side': 'HOLD', 'price': 101.5, 'quantity': 65, 'reason': 'invalid side'},
        ]
        with tempfile.TemporaryDirectory() as td:
            result = execute_paper_trades(candidates, Path(td) / 'executed.csv', max_trades_per_day=1)
        self.assertEqual(result.executed_count, 1)
        self.assertEqual(result.blocked_count, 1)
        self.assertEqual(result.skipped_count, 1)
        self.assertEqual(result.blocked_rows[0]['blocked_reason'], 'MAX_TRADES_PER_DAY')
        self.assertEqual(result.skipped_rows[0]['rejection_reason'], 'INVALID_SIDE')

    def test_paper_execution_summary_returns_metrics_readiness_and_output_views(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'executed.csv'
            path.write_text(
                'strategy,symbol,execution_type,execution_status,signal_time,entry_time,exit_time,pnl,duplicate_reason,validation_error\n'
                'Breakout,NIFTY,PAPER,CLOSED,2026-03-28 09:20:00,2026-03-28 09:20:00,2026-03-28 09:35:00,120,,\n'
                'Breakout,NIFTY,PAPER,CLOSED,2026-03-28 09:40:00,2026-03-28 09:40:00,2026-03-28 09:55:00,-40,,\n',
                encoding='utf-8',
            )
            summary = paper_execution_summary(path, 'Breakout', 'NIFTY', capital=20000.0)
        self.assertEqual(summary['total_trades'], 2)
        self.assertIn('paper_readiness_status', summary)
        self.assertIn('terminal_metrics_lines', summary)
        self.assertIn('dashboard_metrics_rows', summary)
        self.assertTrue(any('Total trades: 2' in line for line in summary['terminal_metrics_lines']))
        self.assertGreater(len(summary['dashboard_metrics_rows']), 0)

    def test_build_trade_evaluation_summary_exposes_records_for_dashboard_or_export(self):
        summary = build_trade_evaluation_summary(
            [
                {'strategy': 'Demand Supply', 'symbol': 'NIFTY', 'execution_type': 'PAPER', 'side': 'BUY', 'signal_time': '2026-03-28 10:00:00', 'entry_time': '2026-03-28 10:00:00', 'exit_time': '2026-03-28 10:20:00', 'entry_price': 100.0, 'exit_price': 101.0, 'pnl': 50.0, 'execution_status': 'CLOSED'},
            ],
            strategy_name='DEMAND_SUPPLY',
        )
        self.assertEqual(summary['strategy'], 'DEMAND_SUPPLY')
        self.assertEqual(len(summary['trade_evaluation_records']), 1)
        self.assertEqual(summary['trade_evaluation_records'][0]['side'], 'BUY')

    def test_blocked_rows_do_not_inflate_trade_count_or_readiness(self):
        summary = build_trade_evaluation_summary(
            [
                {'strategy': 'Breakout', 'symbol': 'NIFTY', 'execution_type': 'PAPER', 'side': 'BUY', 'signal_time': '2026-03-28 09:20:00', 'entry_time': '2026-03-28 09:20:00', 'execution_status': 'EXECUTED', 'trade_status': 'EXECUTED', 'pnl': 0.0},
                {'strategy': 'Breakout', 'symbol': 'NIFTY', 'execution_type': 'PAPER', 'side': 'SELL', 'signal_time': '2026-03-28 09:50:00', 'execution_status': 'BLOCKED', 'trade_status': 'BLOCKED', 'blocked_reason': 'MAX_TRADES_PER_DAY'},
            ],
            strategy_name='BREAKOUT',
            readiness_config=PaperReadinessConfig(min_trades=2),
        )
        self.assertEqual(summary['total_trades'], 1)
        self.assertEqual(summary['paper_ready'], 'NO')

    def test_rejection_reason_maps_into_invalid_trade_count(self):
        summary = build_trade_evaluation_summary(
            [
                {'strategy': 'Breakout', 'symbol': 'NIFTY', 'execution_type': 'PAPER', 'side': 'HOLD', 'signal_time': '2026-03-28 09:35:00', 'rejection_reason': 'INVALID_SIDE'},
            ],
            strategy_name='BREAKOUT',
        )
        self.assertEqual(summary['invalid_trade_count'], 1)

    def test_build_trade_evaluation_summary_emits_strict_live_ready_table(self):
        rows = []
        base_time = datetime(2026, 1, 1, 9, 15)
        for index in range(160):
            trade_time = (base_time + timedelta(hours=index)).strftime('%Y-%m-%d %H:%M:%S')
            pnl = -100.0 if index % 5 == 0 else 220.0
            rows.append(
                {
                    'strategy': 'Breakout',
                    'symbol': 'NIFTY',
                    'execution_type': 'PAPER',
                    'side': 'BUY' if index % 2 == 0 else 'SELL',
                    'signal_time': trade_time,
                    'entry_time': trade_time,
                    'exit_time': trade_time,
                    'entry_price': 100.0,
                    'exit_price': 102.0 if pnl > 0 else 99.0,
                    'pnl': pnl,
                    'execution_status': 'CLOSED',
                }
            )
        summary = build_trade_evaluation_summary(rows, strategy_name='BREAKOUT')
        self.assertEqual(summary['go_live_status'], 'LIVE_READY')
        self.assertEqual(summary['paper_ready'], 'YES')
        self.assertEqual(summary['pre_execution_validation_status'], 'PASS')
        self.assertEqual(summary['post_execution_validation_status'], 'PASS')
        self.assertIn('| Metric | Value | Status | Comment |', summary['strict_validation_markdown'])
        self.assertTrue(any(row['Metric'] == 'Expectancy' for row in summary['strict_validation_rows']))

if __name__ == '__main__':
    unittest.main()
