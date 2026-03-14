import unittest

from src.live_ohlcv import _to_rows


class TestLiveOhlcv(unittest.TestCase):
    def test_to_rows(self):
        payload = {
            "chart": {
                "result": [
                    {
                        "timestamp": [1700000000, 1700000300],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [100.1, 101.2],
                                    "high": [102.0, 102.4],
                                    "low": [99.7, 100.9],
                                    "close": [101.5, 102.1],
                                    "volume": [10000, 12000],
                                }
                            ]
                        },
                    }
                ]
            }
        }
        rows = _to_rows(payload)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["open"], 100.1)
        self.assertEqual(rows[1]["volume"], 12000)


if __name__ == "__main__":
    unittest.main()
