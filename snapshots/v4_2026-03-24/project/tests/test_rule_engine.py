import unittest

from src.rule_engine import evaluate_rule


class TestRuleEngine(unittest.TestCase):
    def test_comparison_ops(self):
        self.assertTrue(evaluate_rule("10", ">", 5))
        self.assertFalse(evaluate_rule("10", "<", 5))
        self.assertTrue(evaluate_rule("10", ">=", 10))
        self.assertTrue(evaluate_rule("10", "<=", 10))
        self.assertTrue(evaluate_rule("10", "==", 10))
        self.assertFalse(evaluate_rule("10", "!=", 10))

    def test_membership_ops(self):
        self.assertTrue(evaluate_rule("SY", "in", ["IR", "KP", "SY"]))
        self.assertTrue(evaluate_rule("US", "not in", ["IR", "KP", "SY"]))


if __name__ == "__main__":
    unittest.main()
