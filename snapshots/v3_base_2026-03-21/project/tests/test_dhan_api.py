import tempfile
import textwrap
import unittest
from pathlib import Path

from src.dhan_api import build_order_request_from_candidate, load_security_map


class TestDhanApi(unittest.TestCase):
    def test_load_security_map_registers_multiple_aliases(self):
        csv_text = textwrap.dedent(
            """\
            alias,symbol,trading_symbol,security_id,exchange_segment,product_type
            NIFTY,NIFTY,NIFTY24MARFUT,12345,NSE_FNO,INTRADAY
            """
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "security_map.csv"
            path.write_text(csv_text, encoding="utf-8")
            security_map = load_security_map(path)

        self.assertIn("NIFTY", security_map)
        self.assertIn("NIFTY24MARFUT", security_map)
        self.assertEqual(security_map["NIFTY"]["security_id"], "12345")

    def test_build_order_request_from_candidate_uses_map_values(self):
        security_map = {
            "NIFTY24MAR24500CE": {
                "security_id": "67890",
                "exchange_segment": "NSE_FNO",
                "product_type": "INTRADAY",
                "order_type": "MARKET",
                "trading_symbol": "NIFTY24MAR24500CE",
                "option_type": "CE",
                "strike_price": "24500",
                "expiry_date": "2026-03-26",
            }
        }
        candidate = {
            "strategy": "BREAKOUT",
            "symbol": "NIFTY",
            "option_strike": "24MAR24500CE",
            "option_type": "CE",
            "strike_price": "24500",
            "side": "BUY",
            "quantity": 50,
        }

        order_request = build_order_request_from_candidate(
            candidate,
            client_id="demo-client",
            security_map=security_map,
        )
        payload = order_request.to_payload()

        self.assertEqual(payload["securityId"], "67890")
        self.assertEqual(payload["exchangeSegment"], "NSE_FNO")
        self.assertEqual(payload["transactionType"], "BUY")
        self.assertEqual(payload["quantity"], 50)
        self.assertEqual(payload["tradingSymbol"], "NIFTY24MAR24500CE")
        self.assertEqual(payload["drvOptionType"], "CE")
        self.assertEqual(payload["drvStrikePrice"], 24500.0)


if __name__ == "__main__":
    unittest.main()
