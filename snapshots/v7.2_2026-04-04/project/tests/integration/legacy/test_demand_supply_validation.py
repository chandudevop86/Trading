import unittest

import pandas as pd

from src.demand_supply_validation import (
    ZoneValidationConfig,
    clear_rejected_zone_log,
    get_rejected_zone_log_frame,
    validate_zone,
)


class TestDemandSupplyValidation(unittest.TestCase):
    def setUp(self) -> None:
        clear_rejected_zone_log()

    def _frame(self, rows):
        return pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    def test_fails_on_missing_columns(self):
        frame = pd.DataFrame([{'timestamp': '2026-03-05 09:15:00', 'open': 100.0}])
        result = validate_zone(
            frame,
            zone_type='demand',
            zone_low=99.0,
            zone_high=100.0,
            created_idx=0,
            touch_idx=0,
            entry_idx=0,
            entry_price=100.0,
            stop_loss=99.0,
            target_price=102.0,
        )
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('invalid_input', result['fail_reasons'])

    def test_fails_weak_move_away_and_no_bos(self):
        frame = self._frame([
            ['2026-03-05 09:15:00', 100.0, 100.3, 99.8, 100.1, 1000],
            ['2026-03-05 09:20:00', 100.1, 100.2, 99.6, 99.8, 900],
            ['2026-03-05 09:25:00', 99.8, 100.0, 99.7, 99.9, 880],
            ['2026-03-05 09:30:00', 99.9, 100.1, 99.8, 100.0, 850],
            ['2026-03-05 09:35:00', 100.0, 100.2, 99.9, 100.1, 830],
        ])
        result = validate_zone(frame, zone_type='demand', zone_low=99.6, zone_high=100.1, created_idx=1, touch_idx=3, entry_idx=4, entry_price=100.1, stop_loss=99.5, target_price=101.0)
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('weak_move_away', result['fail_reasons'])
        self.assertIn('no_break_of_structure', result['fail_reasons'])

    def test_fails_not_fresh_and_late_retest(self):
        frame = self._frame([
            ['2026-03-05 09:15:00', 100.0, 100.2, 99.7, 99.9, 1000],
            ['2026-03-05 09:20:00', 99.9, 100.0, 98.8, 99.1, 1500],
            ['2026-03-05 09:25:00', 99.1, 100.6, 99.0, 100.4, 1800],
            ['2026-03-05 09:30:00', 100.4, 100.5, 99.5, 99.8, 1000],
            ['2026-03-05 09:35:00', 99.8, 100.7, 99.7, 100.5, 1100],
            ['2026-03-05 09:40:00', 100.5, 100.6, 99.6, 99.9, 900],
            ['2026-03-05 09:45:00', 99.9, 100.8, 99.8, 100.6, 1200],
            ['2026-03-05 09:50:00', 100.6, 100.7, 99.7, 100.0, 950],
            ['2026-03-05 09:55:00', 100.0, 100.9, 99.9, 100.7, 1150],
            ['2026-03-05 10:00:00', 100.7, 100.8, 99.8, 100.1, 980],
            ['2026-03-05 10:05:00', 100.1, 100.9, 99.9, 100.8, 1180],
            ['2026-03-05 10:10:00', 100.8, 100.9, 99.7, 100.0, 930],
            ['2026-03-05 10:15:00', 100.0, 101.0, 99.8, 100.9, 1210],
            ['2026-03-05 10:20:00', 100.9, 101.1, 99.7, 100.2, 990],
        ])
        config = ZoneValidationConfig(max_retest_delay=6)
        result = validate_zone(frame, zone_type='demand', zone_low=98.8, zone_high=100.0, created_idx=1, touch_idx=13, entry_idx=13, entry_price=100.2, stop_loss=98.6, target_price=103.6, config=config)
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('not_fresh', result['fail_reasons'])
        self.assertIn('late_retest', result['fail_reasons'])

    def test_fails_bad_rr_and_oversized_zone(self):
        frame = self._frame([
            ['2026-03-05 09:15:00', 102.0, 102.2, 101.7, 102.1, 1200],
            ['2026-03-05 09:20:00', 102.1, 104.2, 101.9, 103.8, 1800],
            ['2026-03-05 09:25:00', 103.8, 103.9, 102.7, 102.9, 1600],
            ['2026-03-05 09:30:00', 102.9, 102.7, 101.7, 102.0, 1500],
            ['2026-03-05 09:35:00', 102.0, 101.9, 101.0, 101.3, 1400],
            ['2026-03-05 09:40:00', 101.3, 103.9, 101.2, 102.4, 1750],
            ['2026-03-05 09:45:00', 102.4, 102.5, 100.8, 100.9, 1900],
        ])
        result = validate_zone(frame, zone_type='supply', zone_low=101.5, zone_high=104.2, created_idx=1, touch_idx=5, entry_idx=6, entry_price=100.9, stop_loss=104.4, target_price=105.5)
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('bad_rr', result['fail_reasons'])
        self.assertIn('oversized_zone', result['fail_reasons'])

    def test_fails_session_and_logs_rejection(self):
        frame = self._frame([
            ['2026-03-05 14:40:00', 100.0, 100.2, 99.8, 100.1, 1000],
            ['2026-03-05 14:45:00', 100.1, 100.2, 98.9, 99.2, 1500],
            ['2026-03-05 14:50:00', 99.2, 100.6, 99.0, 100.4, 1800],
            ['2026-03-05 14:55:00', 100.4, 100.5, 99.2, 99.5, 1100],
            ['2026-03-05 15:00:00', 99.5, 101.0, 99.4, 100.9, 1900],
        ])
        result = validate_zone(frame, zone_type='demand', zone_low=98.9, zone_high=100.1, created_idx=1, touch_idx=3, entry_idx=4, entry_price=100.9, stop_loss=98.7, target_price=105.3)
        self.assertEqual(result['status'], 'FAIL')
        self.assertIn('session_fail', result['fail_reasons'])
        logged = get_rejected_zone_log_frame()
        self.assertEqual(len(logged), 1)
        self.assertIn('session_fail', str(logged.iloc[0]['fail_reasons']))


if __name__ == '__main__':
    unittest.main()
