import sys
import types
import unittest

sys.modules.setdefault('yfinance', types.SimpleNamespace())
sys.modules.setdefault('certifi', types.SimpleNamespace(where=lambda: ''))
sys.modules.setdefault('dateutil', types.SimpleNamespace(parser=types.SimpleNamespace(parse=lambda text: text, ParserError=ValueError)))

from src.auto_backtest import _build_breakout_bias_evaluation, _build_equity_curve_rows, _pnl_summary


class TestAutoBacktestMetrics(unittest.TestCase):
    def test_pnl_summary_includes_risk_metrics(self):
        rows = [
            {'exit_time': '2026-03-20 09:30:00', 'pnl': 100.0, 'gross_pnl': 110.0, 'trading_cost': 10.0},
            {'exit_time': '2026-03-20 10:00:00', 'pnl': -40.0, 'gross_pnl': -35.0, 'trading_cost': 5.0},
            {'exit_time': '2026-03-20 10:30:00', 'pnl': 60.0, 'gross_pnl': 65.0, 'trading_cost': 5.0},
        ]

        summary, curve_rows = _pnl_summary('TEST', rows, starting_equity=1000.0)

        self.assertEqual(summary['wins'], 2)
        self.assertEqual(summary['losses'], 1)
        self.assertEqual(summary['avg_win'], 80.0)
        self.assertEqual(summary['avg_loss'], -40.0)
        self.assertEqual(summary['profit_factor'], 4.0)
        self.assertEqual(summary['max_drawdown'], 40.0)
        self.assertAlmostEqual(summary['max_drawdown_pct'], 3.64, places=2)
        self.assertEqual(summary['ending_equity'], 1120.0)
        self.assertEqual(summary['equity_curve_points'], 3)
        self.assertEqual(curve_rows[-1]['equity'], 1120.0)

    def test_equity_curve_starts_with_initial_capital(self):
        curve_rows = _build_equity_curve_rows(
            'TEST',
            [
                {'exit_time': '2026-03-20 09:30:00', 'pnl': -50.0},
                {'exit_time': '2026-03-20 10:00:00', 'pnl': 25.0},
            ],
            starting_equity=500.0,
        )

        self.assertEqual(curve_rows[0]['equity'], 500.0)
        self.assertEqual(curve_rows[1]['equity'], 450.0)
        self.assertEqual(curve_rows[1]['drawdown'], 50.0)
        self.assertEqual(curve_rows[2]['equity'], 475.0)

    def test_profit_factor_is_inf_when_no_losses(self):
        summary, _ = _pnl_summary(
            'TEST',
            [
                {'exit_time': '2026-03-20 09:30:00', 'pnl': 25.0},
                {'exit_time': '2026-03-20 10:00:00', 'pnl': 35.0},
            ],
            starting_equity=1000.0,
        )

        self.assertEqual(summary['profit_factor'], 'INF')
        self.assertEqual(summary['max_drawdown'], 0.0)

    def test_breakout_bias_evaluation_reports_better_mode_and_deltas(self):
        comparison = _build_breakout_bias_evaluation(
            {'strategy': 'BREAKOUT', 'total_pnl': 1250.0, 'win_rate_pct': 60.0, 'trades': 5},
            {'strategy': 'BREAKOUT_NO_BIAS', 'total_pnl': 900.0, 'win_rate_pct': 50.0, 'trades': 7},
        )

        self.assertEqual(comparison['mode_a'], 'BREAKOUT')
        self.assertEqual(comparison['mode_b'], 'BREAKOUT_NO_BIAS')
        self.assertEqual(comparison['better_mode'], 'BIAS_REQUIRED')
        self.assertEqual(comparison['pnl_delta'], 350.0)
        self.assertEqual(comparison['win_rate_delta_pct'], 10.0)
        self.assertEqual(comparison['trades_delta'], -2)


if __name__ == '__main__':
    unittest.main()
