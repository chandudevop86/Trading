import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.reporting_service import current_execution_rows, paper_execution_summary, recent_trade_summary, status_message


class TestReportingService(unittest.TestCase):
    def test_recent_trade_summary_formats_last_trade(self):
        text = recent_trade_summary([
            {'side': 'BUY', 'strategy': 'BREAKOUT', 'entry_price': 101.25, 'stop_loss': 99.5, 'target_price': 104.75, 'score': 6.2}
        ])
        self.assertIn('BUY BREAKOUT', text)
        self.assertIn('Entry 101.25', text)

    def test_status_message_prefers_backtest(self):
        self.assertEqual(status_message(run_clicked=True, backtest_clicked=True), 'Backtest completed')
        self.assertEqual(status_message(run_clicked=True, backtest_clicked=False), 'Run completed')

    def test_current_execution_rows_filters_strategy_symbol_and_mode(self):
        with TemporaryDirectory() as td:
            path = Path(td) / 'executed.csv'
            path.write_text(
                'strategy,symbol,execution_type,execution_status\n'
                'Breakout,NIFTY,PAPER,EXECUTED\n'
                'Breakout,BANKNIFTY,PAPER,EXECUTED\n'
                'Breakout,NIFTY,LIVE,FILLED\n',
                encoding='utf-8',
            )
            rows = current_execution_rows(path, 'Breakout', 'NIFTY', execution_type='PAPER')
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['symbol'], 'NIFTY')

    def test_paper_execution_summary_counts_open_executed_rows_when_no_closed_trades_exist(self):
        with TemporaryDirectory() as td:
            path = Path(td) / 'executed.csv'
            path.write_text(
                'strategy,symbol,execution_type,execution_status,executed_at_utc,pnl\n'
                'Breakout,NIFTY,PAPER,EXECUTED,2026-03-27 09:30:00,0\n',
                encoding='utf-8',
            )
            summary = paper_execution_summary(path, 'Breakout', 'NIFTY', capital=20000.0)
            self.assertEqual(summary['total_trades'], 1)
            self.assertEqual(summary['open_trades'], 1)
            self.assertEqual(summary['closed_trades'], 0)
            self.assertEqual(summary['total_pnl'], 0.0)


if __name__ == '__main__':
    unittest.main()

