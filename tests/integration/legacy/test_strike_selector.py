import unittest

from src.strike_selector import attach_option_strikes, pick_option_strike


class TestStrikeSelector(unittest.TestCase):
    def test_buy_atm_returns_ce(self):
        strike, option_type = pick_option_strike(spot_price=22337.0, side="BUY", step=50, moneyness="ATM", steps=1)
        self.assertEqual(strike, 22350)
        self.assertEqual(option_type, "CE")

    def test_sell_otm_returns_lower_pe_strike(self):
        strike, option_type = pick_option_strike(spot_price=22337.0, side="SELL", step=50, moneyness="OTM", steps=2)
        self.assertEqual(strike, 22250)
        self.assertEqual(option_type, "PE")

    def test_attach_option_strikes(self):
        trades = [
            {"side": "BUY", "entry_price": 22412.2, "pnl": 100.0},
            {"side": "SELL", "entry_price": 22412.2, "pnl": -50.0},
        ]

        out = attach_option_strikes(trades, strike_step=50, moneyness="ATM", steps=1)

        self.assertEqual(out[0]["option_type"], "CE")
        self.assertEqual(out[1]["option_type"], "PE")
        self.assertEqual(out[0]["strike_price"], 22400)
        self.assertEqual(out[1]["strike_price"], 22400)


if __name__ == "__main__":
    unittest.main()
