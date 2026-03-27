import unittest

from src.runtime_strategy_presets import normalize_runtime_mode, operator_default_values, runtime_strategy_kwargs


class TestRuntimeStrategyPresets(unittest.TestCase):
    def test_normalize_runtime_mode_defaults_to_balanced(self):
        self.assertEqual(normalize_runtime_mode('balanced'), 'Balanced')
        self.assertEqual(normalize_runtime_mode(''), 'Balanced')
        self.assertEqual(normalize_runtime_mode('weird'), 'Balanced')

    def test_operator_defaults_and_runtime_kwargs_use_balanced_mode(self):
        defaults = operator_default_values()
        kwargs = runtime_strategy_kwargs('balanced')
        self.assertEqual(defaults['mode'], 'Balanced')
        self.assertEqual(kwargs['amd_mode'], 'Balanced')


if __name__ == '__main__':
    unittest.main()
