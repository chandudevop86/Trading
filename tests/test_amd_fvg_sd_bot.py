import unittest

from src.amd_fvg_sd_bot import ConfluenceConfig, score_trade_setup


class TestAmdFvgSdBotScoring(unittest.TestCase):
    def test_score_trade_setup_uses_weighted_threshold_logic(self):
        config = ConfluenceConfig(
            mode='Balanced',
            min_score_conservative=7.0,
            min_score_balanced=5.0,
            min_score_aggressive=3.0,
            require_liquidity_sweep=False,
            require_fvg_confirmation=False,
            require_distribution_phase=False,
            minimum_amd_confidence=0.0,
        )

        score = score_trade_setup(
            {
                'amd_confidence': 1.2,
                'liquidity_sweep': True,
                'has_fvg': True,
                'has_bvg': True,
                'zone_proximity': True,
                'retest_confirmation': False,
                'trend_alignment': False,
            },
            config,
            'Balanced',
        )

        self.assertEqual(score['trend_score'], 2.0)
        self.assertEqual(score['sweep_score'], 1.0)
        self.assertEqual(score['imbalance_score'], 2.0)
        self.assertEqual(score['zone_score'], 2.0)
        self.assertEqual(score['total_score'], 7.0)
        self.assertTrue(score['accepted'])

    def test_score_trade_setup_counts_vwap_alignment(self):
        config = ConfluenceConfig(
            mode='Balanced',
            min_score_conservative=7.0,
            min_score_balanced=5.0,
            min_score_aggressive=3.0,
            require_liquidity_sweep=False,
            require_fvg_confirmation=False,
            require_distribution_phase=False,
            minimum_amd_confidence=0.0,
        )

        accepted = score_trade_setup(
            {
                'amd_confidence': 1.2,
                'liquidity_sweep': True,
                'has_fvg': True,
                'has_bvg': False,
                'zone_proximity': True,
                'trend_alignment': True,
                'vwap_alignment': True,
                'retest_confirmation': True,
            },
            config,
            'Balanced',
        )
        rejected = score_trade_setup(
            {
                'amd_confidence': 0.0,
                'liquidity_sweep': False,
                'has_fvg': False,
                'has_bvg': False,
                'zone_proximity': True,
                'trend_alignment': True,
                'vwap_alignment': False,
                'retest_confirmation': True,
            },
            config,
            'Balanced',
        )

        self.assertGreater(accepted['total_score'], rejected['total_score'])
        self.assertTrue(accepted['accepted'])

    def test_score_trade_setup_requires_strict_confluence_when_enabled(self):
        config = ConfluenceConfig(
            mode='Balanced',
            min_score_conservative=7.0,
            min_score_balanced=5.0,
            min_score_aggressive=3.0,
            require_liquidity_sweep=True,
            require_fvg_confirmation=True,
            require_distribution_phase=True,
            minimum_amd_confidence=1.2,
        )

        score = score_trade_setup(
            {
                'amd_phase': 'manipulation',
                'amd_confidence': 0.9,
                'liquidity_sweep': False,
                'has_fvg': False,
                'has_bvg': True,
                'zone_proximity': True,
                'trend_alignment': True,
                'vwap_alignment': True,
                'retest_confirmation': True,
            },
            config,
            'Balanced',
        )

        self.assertFalse(score['accepted'])
        self.assertIn('missing_required_sweep', score['rejection_reason'])
        self.assertIn('missing_required_fvg', score['rejection_reason'])
        self.assertIn('missing_distribution_phase', score['rejection_reason'])
        self.assertIn('amd_confidence_below_1.20', score['rejection_reason'])

    def test_score_trade_setup_rejects_when_score_is_below_threshold(self):
        config = ConfluenceConfig(
            mode='Balanced',
            min_score_conservative=7.0,
            min_score_balanced=5.0,
            min_score_aggressive=3.0,
            require_liquidity_sweep=False,
            require_fvg_confirmation=False,
            require_distribution_phase=False,
            minimum_amd_confidence=0.1,
        )

        score = score_trade_setup(
            {
                'amd_confidence': 0.0,
                'liquidity_sweep': False,
                'has_fvg': False,
                'has_bvg': False,
                'zone_proximity': True,
                'trend_alignment': False,
                'retest_confirmation': False,
            },
            config,
            'Balanced',
        )

        self.assertEqual(score['total_score'], 2.0)
        self.assertFalse(score['accepted'])
        self.assertIn('missing_trend', score['rejection_reason'])
        self.assertIn('missing_fvg', score['rejection_reason'])
        self.assertIn('score_below_5.00', score['rejection_reason'])


if __name__ == '__main__':
    unittest.main()
