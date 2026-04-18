from __future__ import annotations

"""Canonical NSE option-chain helpers for app runtime."""

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
    normalized = (symbol or "").strip().upper()
    if normalized in {"^NSEI", "NIFTY", "NIFTY 50"}:
        return "NIFTY"
    if normalized in {"^NSEBANK", "BANKNIFTY", "NIFTY BANK"}:
        return "BANKNIFTY"
    return normalized.replace("^", "")


def fetch_option_chain(symbol: str, timeout: float = 10.0) -> dict[str, Any]:
    normalized_symbol = normalize_index_symbol(symbol)
    jar = CookieJar()
    https_context = ssl.create_default_context()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

    req0 = urllib.request.Request(_BASE_URL, headers=_HEADERS, method="GET")
    with opener.open(req0, timeout=timeout, context=https_context):
        pass

    url = _CHAIN_URL.format(symbol=urllib.parse.quote(normalized_symbol))
    req = urllib.request.Request(url, headers=_HEADERS, method="GET")
    with opener.open(req, timeout=timeout, context=https_context) as resp:
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
        for option_type in ("CE", "PE"):
            leg = row.get(option_type)
            if not isinstance(leg, dict):
                continue
            records.append(
                {
                    "strikePrice": strike,
                    "optionType": option_type,
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
    for record in records:
        try:
            strike = int(float(record.get("strikePrice")))
        except Exception:
            continue
        option_type = str(record.get("optionType", "")).upper()
        if option_type not in {"CE", "PE"}:
            continue
        out[(strike, option_type)] = {
            "option_ltp": record.get("lastPrice"),
            "option_oi": record.get("openInterest"),
            "option_vol": record.get("totalTradedVolume"),
            "option_iv": record.get("impliedVolatility"),
            "option_expiry": record.get("expiryDate"),
        }
    return out


__all__ = [
    "build_metrics_map",
    "extract_option_records",
    "fetch_option_chain",
    "normalize_index_symbol",
]
