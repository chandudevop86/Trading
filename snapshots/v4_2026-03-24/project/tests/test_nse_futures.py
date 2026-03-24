import unittest

from src.nse_futures import extract_futures_records, normalize_futures_symbol


class TestNseFutures(unittest.TestCase):
    def test_normalize_futures_symbol(self):
        self.assertEqual(normalize_futures_symbol("^NSEI"), "NIFTY")
        self.assertEqual(normalize_futures_symbol("nifty futures"), "NIFTY")
        self.assertEqual(normalize_futures_symbol("BANKNIFTYFUT"), "BANKNIFTY")

    def test_extract_futures_records(self):
        payload = {
            "stocks": [
                {
                    "instrumentType": "FUTIDX",
                    "underlying": "NIFTY",
                    "expiryDate": "26-Mar-2026",
                    "lastPrice": 22451.5,
                    "change": 31.2,
                    "pChange": 0.14,
                    "openPrice": 22400.0,
                    "highPrice": 22488.0,
                    "lowPrice": 22370.0,
                    "previousClose": 22420.3,
                    "openInterest": 123456,
                    "changeinOpenInterest": 7890,
                    "numberOfContractsTraded": 45678,
                    "totalTurnover": 987654321,
                    "underlyingValue": 22420.8,
                    "marketLot": 65,
                },
                {
                    "instrumentType": "OPTIDX",
                    "underlying": "NIFTY",
                    "expiryDate": "26-Mar-2026",
                },
            ]
        }

        rows = extract_futures_records(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "NIFTY")
        self.assertEqual(rows[0]["instrumentType"], "FUTIDX")
        self.assertEqual(rows[0]["openInterest"], 123456)
        self.assertEqual(rows[0]["marketLot"], 65)


if __name__ == "__main__":
    unittest.main()
