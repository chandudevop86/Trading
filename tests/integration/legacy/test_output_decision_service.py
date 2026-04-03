import unittest

from src.output_decision_service import build_plain_english_next_action


class TestOutputDecisionService(unittest.TestCase):
    def test_plain_english_next_action_uses_combined_trade_and_expectancy_message(self):
        message = build_plain_english_next_action(
            {
                'deployment_blockers': 'MIN_TRADES<150;NEGATIVE_EXPECTANCY',
            }
        )

        self.assertEqual(
            message,
            'This strategy is not ready yet because not enough trades yet, expectancy is not positive. Run a clean backtest until the system has 150 to 200 validated trades.',
        )


if __name__ == '__main__':
    unittest.main()
