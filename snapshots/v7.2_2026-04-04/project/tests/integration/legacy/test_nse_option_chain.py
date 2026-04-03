import unittest

from src.nse_option_chain import build_metrics_map, extract_option_records


class TestNseOptionChain(unittest.TestCase):
    def test_extract_and_map(self):
        payload = {
            "records": {
                "data": [
                    {
                        "strikePrice": 23100,
                        "expiryDate": "2026-03-26",
                        "underlyingValue": 23110.0,
                        "CE": {
                            "lastPrice": 120.5,
                            "openInterest": 1000,
                            "totalTradedVolume": 500,
                            "impliedVolatility": 12.3,
                        },
                        "PE": {
                            "lastPrice": 98.0,
                            "openInterest": 2000,
                            "totalTradedVolume": 600,
                            "impliedVolatility": 13.1,
                        },
                    }
                ]
            }
        }
        records = extract_option_records(payload)
        self.assertEqual(len(records), 2)
        metrics = build_metrics_map(records)
        self.assertIn((23100, "CE"), metrics)
        self.assertIn((23100, "PE"), metrics)
        self.assertEqual(metrics[(23100, "CE")]["option_oi"], 1000)
        self.assertEqual(metrics[(23100, "PE")]["option_ltp"], 98.0)
        self.assertEqual(metrics[(23100, "CE")]["option_expiry"], "2026-03-26")


if __name__ == "__main__":
    unittest.main()


