import unittest

from src.strategy_tuning import apply_strategy_benchmark, optimizer_report_rows, strategy_tuning_preset


class TestStrategyTuning(unittest.TestCase):
    def test_strategy_preset_returns_expected_breakout_values(self):
        preset = strategy_tuning_preset('Breakout')
        self.assertEqual(preset.balanced_threshold, 5.5)
        self.assertEqual(preset.max_trades_per_day, 2)
        self.assertEqual(preset.min_profit_factor, 1.35)

    def test_apply_strategy_benchmark_sets_pass_fail(self):
        passed = apply_strategy_benchmark(
            {
                'strategy': 'AMD_FVG_SD',
                'total_trades': 120,
                'win_rate': 44.0,
                'profit_factor': 1.55,
                'expectancy_per_trade': 8.0,
                'avg_rr': 1.5,
                'max_drawdown_pct': 8.0,
                'duplicate_rejections': 2,
                'risk_rule_rejections': 3,
            }
        )
        failed = apply_strategy_benchmark(
            {
                'strategy': 'INDICATOR',
                'total_trades': 80,
                'win_rate': 35.0,
                'profit_factor': 0.95,
                'expectancy_per_trade': -3.0,
                'avg_rr': 0.8,
                'max_drawdown_pct': 18.0,
                'duplicate_rejections': 12,
                'risk_rule_rejections': 15,
            }
        )
        self.assertEqual(passed['deployment_ready'], 'YES')
        self.assertEqual(failed['deployment_ready'], 'NO')
        self.assertIn('MIN_TRADES<100', failed['deployment_blockers'])

    def test_optimizer_report_rows_orders_by_rank_score(self):
        rows = optimizer_report_rows(
            [
                {
                    'strategy': 'BREAKOUT',
                    'deployment_ready': 'NO',
                    'positive_expectancy': 'NO',
                    'expectancy_per_trade': -1.0,
                    'profit_factor': 0.9,
                    'avg_rr': 0.9,
                    'win_rate': 38.0,
                    'max_drawdown_pct': 14.0,
                },
                {
                    'strategy': 'AMD_FVG_SD',
                    'deployment_ready': 'YES',
                    'positive_expectancy': 'YES',
                    'expectancy_per_trade': 5.0,
                    'profit_factor': 1.6,
                    'avg_rr': 1.4,
                    'win_rate': 45.0,
                    'max_drawdown_pct': 8.0,
                },
            ]
        )
        self.assertEqual(rows[0]['strategy'], 'AMD_FVG_SD')
        self.assertEqual(rows[0]['optimizer_rank'], 1)


if __name__ == '__main__':
    unittest.main()
