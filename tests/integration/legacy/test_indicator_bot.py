import unittest

from src.breakout_bot import load_candles
from src.indicator_bot import IndicatorConfig, build_indicator_summary, generate_indicator_rows, generate_trades


class TestIndicatorBot(unittest.TestCase):
    def test_generates_indicator_rows(self):
        rows = []
        price = 100.0
        for i in range(60):
            o = price
            h = price + 1.2
            l = price - 0.8
            c = price + 0.6
            v = 1000 + i * 10
            hh = 9 + ((15 + i * 15) // 60)
            mm = (15 + i * 15) % 60
            rows.append(
                {
                    "timestamp": f"2026-03-01 {hh:02d}:{mm:02d}:00",
                    "open": str(round(o, 2)),
                    "high": str(round(h, 2)),
                    "low": str(round(l, 2)),
                    "close": str(round(c, 2)),
                    "volume": str(v),
                }
            )
            price += 0.5

        candles = load_candles(rows)
        out = generate_indicator_rows(candles)

        self.assertEqual(len(out), len(rows))
        last = out[-1]
        self.assertIn("rsi", last)
        self.assertIn("adx", last)
        self.assertIn("macd", last)
        self.assertIn("vwap", last)
        self.assertIn(last["market_signal"], {"BULLISH_TREND", "OVERBOUGHT", "NEUTRAL", "RANGE", "BEARISH_TREND", "OVERSOLD", "INSUFFICIENT_DATA"})

    def test_config_changes_signal_thresholds(self):
        rows = []
        price = 100.0
        for i in range(50):
            rows.append(
                {
                    "timestamp": f"2026-03-02 {9 + ((15 + i * 15) // 60):02d}:{(15 + i * 15) % 60:02d}:00",
                    "open": str(price),
                    "high": str(price + 1),
                    "low": str(price - 1),
                    "close": str(price + 0.7),
                    "volume": "1000",
                }
            )
            price += 0.35

        candles = load_candles(rows)
        strict = IndicatorConfig(rsi_overbought=65.0, rsi_oversold=35.0, adx_trend_min=18.0)
        out = generate_indicator_rows(candles, config=strict)
        self.assertTrue(any(r["market_signal"] in {"OVERBOUGHT", "BULLISH_TREND"} for r in out))

    def test_generate_trades_emits_scored_indicator_setups(self):
        rows = []
        price = 100.0
        for i in range(90):
            rows.append(
                {
                    "timestamp": f"2026-03-03 {9 + ((15 + i * 5) // 60):02d}:{(15 + i * 5) % 60:02d}:00",
                    "open": str(round(price, 2)),
                    "high": str(round(price + 1.2, 2)),
                    "low": str(round(price - 0.4, 2)),
                    "close": str(round(price + 0.9, 2)),
                    "volume": str(1000 + i * 25),
                }
            )
            price += 0.45

        candles = load_candles(rows)
        trades = generate_trades(candles, capital=100000.0, risk_pct=0.01, rr_ratio=2.0, config=IndicatorConfig(mode='Aggressive', min_score_threshold=4.0, allow_reversal_signals=True, require_trend_alignment=False, duplicate_signal_cooldown_bars=1))

        self.assertTrue(trades)
        first = trades[0]
        self.assertIn(first['score_bucket'], {'A', 'B', 'C', 'D'})
        self.assertEqual(first['indicator_grade'], first['score_bucket'])
        self.assertGreaterEqual(float(first['strict_validation_score']), 4.0)
        self.assertEqual(first['validation_status'], 'PASS')
    def test_build_indicator_summary(self):
        summary = build_indicator_summary(
            [
                {
                    "timestamp": "2026-03-01 10:15:00",
                    "market_signal": "BULLISH_TREND",
                    "trend_strength": "STRONG",
                    "rsi": 62.1,
                    "adx": 28.7,
                    "macd": 1.22,
                    "macd_signal": 0.95,
                    "close": 22150.0,
                    "vwap": 22110.5,
                }
            ]
        )
        self.assertIn("Indicator Bot Alert", summary)
        self.assertIn("BULLISH_TREND", summary)


if __name__ == "__main__":
    unittest.main()



