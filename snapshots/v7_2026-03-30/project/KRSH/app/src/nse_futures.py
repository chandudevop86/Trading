from __future__ import annotations

import json
import ssl
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from typing import Any


_BASE_URL = "https://www.nseindia.com/"
_QUOTE_DERIVATIVE_URL = "https://www.nseindia.com/api/quote-derivative?symbol={symbol}"

_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "accept": "application/json,text/plain,*/*",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://www.nseindia.com/get-quotes/derivatives",
}


def _decode_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def normalize_futures_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s in {"^NSEI", "NIFTY", "NIFTY 50", "NIFTY50", "NIFTY FUT", "NIFTY FUTURES", "NIFTYFUT"}:
        return "NIFTY"
    if s in {"^NSEBANK", "BANKNIFTY", "NIFTY BANK", "BANKNIFTY FUT", "BANKNIFTY FUTURES", "BANKNIFTYFUT"}:
        return "BANKNIFTY"
    return s.replace("^", "")


def fetch_futures_chain(symbol: str, timeout: float = 10.0) -> dict[str, Any]:
    sym = normalize_futures_symbol(symbol)
    jar = CookieJar()
    https_ctx = ssl.create_default_context()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    req0 = urllib.request.Request(_BASE_URL, headers=_HEADERS, method="GET")
    with opener.open(req0, timeout=timeout, context=https_ctx):
        pass

    url = _QUOTE_DERIVATIVE_URL.format(symbol=urllib.parse.quote(sym))
    req = urllib.request.Request(url, headers=_HEADERS, method="GET")
    with opener.open(req, timeout=timeout, context=https_ctx) as resp:
        text = _decode_bytes(resp.read())
        return json.loads(text)


def _pick_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in {None, ""}:
            return row.get(key)
    return ""


def extract_futures_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    contracts = payload.get("stocks")
    if not isinstance(contracts, list):
        return []

    records: list[dict[str, Any]] = []
    for row in contracts:
        if not isinstance(row, dict):
            continue

        instrument = str(_pick_value(row, "instrumentType")).upper()
        if "FUT" not in instrument:
            continue

        records.append(
            {
                "symbol": _pick_value(row, "underlying", "symbol"),
                "instrumentType": _pick_value(row, "instrumentType"),
                "expiryDate": _pick_value(row, "expiryDate"),
                "lastPrice": _pick_value(row, "lastPrice", "ltp"),
                "change": _pick_value(row, "change"),
                "pChange": _pick_value(row, "pChange"),
                "openPrice": _pick_value(row, "openPrice"),
                "highPrice": _pick_value(row, "highPrice"),
                "lowPrice": _pick_value(row, "lowPrice"),
                "previousClose": _pick_value(row, "previousClose", "closePrice"),
                "openInterest": _pick_value(row, "openInterest"),
                "changeInOpenInterest": _pick_value(row, "changeinOpenInterest", "changeInOpenInterest"),
                "numberOfContractsTraded": _pick_value(row, "numberOfContractsTraded", "contractsTraded"),
                "totalTurnover": _pick_value(row, "totalTurnover"),
                "underlyingValue": _pick_value(row, "underlyingValue"),
                "marketLot": _pick_value(row, "marketLot", "marketLotSize"),
            }
        )
    return records
