from __future__ import annotations

import json
import ssl
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from typing import Any


_BASE_URL = "https://www.nseindia.com/"
_CHAIN_URL = "https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"

_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "accept": "application/json,text/plain,*/*",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://www.nseindia.com/option-chain",
}


def _decode_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def normalize_index_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s in {"^NSEI", "NIFTY", "NIFTY 50"}:
        return "NIFTY"
    if s in {"^NSEBANK", "BANKNIFTY", "NIFTY BANK"}:
        return "BANKNIFTY"
    return s.replace("^", "")


def fetch_option_chain(symbol: str, timeout: float = 10.0) -> dict[str, Any]:
    sym = normalize_index_symbol(symbol)
    jar = CookieJar()
    https_ctx = ssl.create_default_context()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    # NSE often requires initial cookies from the homepage before API calls work.
    req0 = urllib.request.Request(_BASE_URL, headers=_HEADERS, method="GET")
    with opener.open(req0, timeout=timeout, context=https_ctx):
        pass

    url = _CHAIN_URL.format(symbol=urllib.parse.quote(sym))
    req = urllib.request.Request(url, headers=_HEADERS, method="GET")
    with opener.open(req, timeout=timeout, context=https_ctx) as resp:
        text = _decode_bytes(resp.read())
        return json.loads(text)


def extract_option_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    raw = payload.get("records", {})
    data = raw.get("data", []) if isinstance(raw, dict) else []
    for row in data:
        if not isinstance(row, dict):
            continue
        strike = row.get("strikePrice")
        expiry = row.get("expiryDate")
        underlying = row.get("underlyingValue")
        for opt_type in ("CE", "PE"):
            leg = row.get(opt_type)
            if not isinstance(leg, dict):
                continue
            records.append(
                {
                    "strikePrice": strike,
                    "optionType": opt_type,
                    "expiryDate": expiry,
                    "underlyingValue": underlying,
                    "lastPrice": leg.get("lastPrice"),
                    "openInterest": leg.get("openInterest"),
                    "totalTradedVolume": leg.get("totalTradedVolume"),
                    "impliedVolatility": leg.get("impliedVolatility"),
                }
            )
    return records


def build_metrics_map(records: list[dict[str, Any]]) -> dict[tuple[int, str], dict[str, Any]]:
    out: dict[tuple[int, str], dict[str, Any]] = {}
    for r in records:
        try:
            strike = int(float(r.get("strikePrice")))
        except Exception:
            continue
        opt = str(r.get("optionType", "")).upper()
        if opt not in {"CE", "PE"}:
            continue
        out[(strike, opt)] = {
            "option_ltp": r.get("lastPrice"),
            "option_oi": r.get("openInterest"),
            "option_vol": r.get("totalTradedVolume"),
            "option_iv": r.get("impliedVolatility"),
            "option_expiry": r.get("expiryDate"),
        }
    return out

