import tempfile
import textwrap
import unittest
from pathlib import Path

from src.dhan_api import (
    INVALID_EXPIRY,
    INVALID_STRIKE,
    SECURITY_MAP_NOT_LOADED,
    DhanExecutionError,
    build_order_request_from_candidate,
    find_option_instrument,
    load_security_map,
    normalize_trading_symbol,
    resolve_security,
)


CSV_TEXT = textwrap.dedent(
    """\
    security_id,exchange,segment,exchange_segment,instrument_type,symbol_name,display_name,underlying_symbol,underlying_security_id,expiry_date,strike_price,option_type,lot_size
    1001,NSE,E,IDX_I,INDEX,NIFTY,NIFTY,NIFTY,, , , ,1
    1002,NSE,E,IDX_I,INDEX,BANKNIFTY,BANKNIFTY,BANKNIFTY,, , , ,1
    2001,NSE,D,NSE_FNO,OPTIDX,NIFTY,NIFTY 2026-03-26 22500 CE,NIFTY,1001,2026-03-26,22500,CE,75
    2002,NSE,D,NSE_FNO,OPTIDX,NIFTY,NIFTY 2026-03-26 22500 PE,NIFTY,1001,2026-03-26,22500,PE,75
    2003,NSE,D,NSE_FNO,OPTIDX,BANKNIFTY,BANKNIFTY 2026-03-26 48000 CE,BANKNIFTY,1002,2026-03-26,48000,CE,35
    """
)


class TestDhanApi(unittest.TestCase):
    def _load_security_map(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        path = Path(tmpdir.name) / "security_map.csv"
        path.write_text(CSV_TEXT, encoding="utf-8")
        return load_security_map(path)

    def test_normalize_trading_symbol_maps_yahoo_indexes(self):
        self.assertEqual(normalize_trading_symbol("^NSEI"), "NIFTY")
        self.assertEqual(normalize_trading_symbol("^NSEBANK"), "BANKNIFTY")

    def test_resolve_security_preserves_data_symbol_and_trade_symbol(self):
        security_map = self._load_security_map()
        resolved = resolve_security({"symbol": "^NSEI"}, security_map)
        self.assertEqual(resolved["data_symbol"], "^NSEI")
        self.assertEqual(resolved["trade_symbol"], "NIFTY")
        self.assertEqual(resolved["security_id"], "1001")

    def test_find_option_instrument_resolves_ce_and_pe(self):
        security_map = self._load_security_map()
        ce = find_option_instrument(security_map, "NIFTY", "2026-03-26", 22500, "CE")
        pe = find_option_instrument(security_map, "NIFTY", "2026-03-26", 22500, "PE")
        self.assertIsNotNone(ce)
        self.assertIsNotNone(pe)
        self.assertEqual(ce["security_id"], "2001")
        self.assertEqual(pe["security_id"], "2002")

    def test_build_order_request_from_candidate_creates_v2_payload(self):
        security_map = self._load_security_map()
        candidate = {
            "strategy": "BREAKOUT",
            "symbol": "^NSEI",
            "side": "BUY",
            "quantity": 75,
            "option_expiry": "2026-03-26",
            "strike_price": 22500,
            "option_type": "CE",
            "order_type": "MARKET",
            "product_type": "INTRADAY",
        }
        order_request = build_order_request_from_candidate(candidate, client_id="1000001", security_map=security_map)
        payload = order_request.to_payload()
        self.assertEqual(payload["dhanClientId"], "1000001")
        self.assertEqual(payload["securityId"], "2001")
        self.assertEqual(payload["exchangeSegment"], "NSE_FNO")
        self.assertEqual(payload["transactionType"], "BUY")
        self.assertEqual(payload["quantity"], 75)
        self.assertEqual(payload["orderType"], "MARKET")
        self.assertEqual(payload["productType"], "INTRADAY")
        self.assertEqual(payload["drvExpiryDate"], "2026-03-26")
        self.assertEqual(payload["drvOptionType"], "CALL")
        self.assertEqual(payload["drvStrikePrice"], 22500.0)

    def test_resolve_security_fails_when_expiry_missing(self):
        security_map = self._load_security_map()
        with self.assertRaises(DhanExecutionError) as ctx:
            resolve_security({"symbol": "^NSEI", "strike_price": 22500, "option_type": "CE"}, security_map)
        self.assertEqual(ctx.exception.code, INVALID_EXPIRY)

    def test_resolve_security_fails_when_strike_missing(self):
        security_map = self._load_security_map()
        with self.assertRaises(DhanExecutionError) as ctx:
            resolve_security({"symbol": "^NSEBANK", "option_expiry": "2026-03-26", "option_type": "CE"}, security_map)
        self.assertEqual(ctx.exception.code, INVALID_STRIKE)

    def test_build_order_request_fails_when_security_map_missing(self):
        with self.assertRaises(DhanExecutionError) as ctx:
            build_order_request_from_candidate({"symbol": "^NSEI", "side": "BUY", "quantity": 1}, client_id="1000001", security_map=None)
        self.assertEqual(ctx.exception.code, SECURITY_MAP_NOT_LOADED)


if __name__ == "__main__":
    unittest.main()
