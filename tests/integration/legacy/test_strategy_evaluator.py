import unittest

from src.strategy_evaluator import rank_strategy_summaries


class TestStrategyEvaluator(unittest.TestCase):
    def test_ranks_positive_expectancy_before_trade_count(self):
        ranked = rank_strategy_summaries(
            [
                {
                    'strategy': 'HIGH_FREQ',
                    'expectancy_per_trade': -2.0,
                    'profit_factor': 0.9,
                    'max_drawdown': 50.0,
                    'win_rate': 62.0,
                    'total_trades': 40,
                },
                {
                    'strategy': 'EDGE_FIRST',
                    'expectancy_per_trade': 4.5,
                    'profit_factor': 1.8,
                    'max_drawdown': 25.0,
                    'win_rate': 48.0,
                    'total_trades': 12,
                },
            ]
        )
        self.assertEqual(ranked[0]['strategy'], 'EDGE_FIRST')
        self.assertEqual(ranked[0]['rank'], 1)
        self.assertEqual(ranked[0]['positive_expectancy'], 'YES')
        self.assertEqual(ranked[1]['positive_expectancy'], 'NO')

    def test_uses_profit_factor_then_drawdown_as_tiebreakers(self):
        ranked = rank_strategy_summaries(
            [
                {
                    'strategy': 'LOW_DD',
                    'expectancy_per_trade': 3.0,
                    'profit_factor': 1.6,
                    'max_drawdown': 20.0,
                    'win_rate': 51.0,
                    'total_trades': 10,
                },
                {
                    'strategy': 'HIGH_PF',
                    'expectancy_per_trade': 3.0,
                    'profit_factor': 2.2,
                    'max_drawdown': 35.0,
                    'win_rate': 55.0,
                    'total_trades': 14,
                },
            ]
        )
        self.assertEqual(ranked[0]['strategy'], 'HIGH_PF')
        self.assertEqual(ranked[1]['strategy'], 'LOW_DD')


if __name__ == '__main__':
    unittest.main()
