import unittest
from unittest.mock import patch

from src.nse_client import fetch_nifty50_rows


class TestNseClient(unittest.TestCase):
    @patch(
        "src.nse_client._fetch_csv_text",
        return_value=(
            "Company Name,Industry,Symbol,Series,ISIN Code\n"
            "Reliance Industries Ltd.,Oil Gas & Consumable Fuels,RELIANCE,EQ,INE002A01018\n"
        ),
    )
    def test_fetch_nifty50_rows(self, _):
        rows = fetch_nifty50_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "RELIANCE")
        self.assertEqual(rows[0]["series"], "EQ")


if __name__ == "__main__":
    unittest.main()
