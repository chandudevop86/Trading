from __future__ import annotations

import csv
import io
import urllib.request
from typing import Any

CONSTITUENTS_CSV_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv"

HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "accept": "text/csv,application/octet-stream,*/*",
    "referer": "https://www.niftyindices.com/",
}


def _decode_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _fetch_csv_text(url: str) -> str:
    request = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=30) as response:
        return _decode_bytes(response.read())


def fetch_nifty50_rows() -> list[dict[str, Any]]:
    csv_text = _fetch_csv_text(CONSTITUENTS_CSV_URL)
    reader = csv.DictReader(io.StringIO(csv_text))

    rows: list[dict[str, Any]] = []
    for item in reader:
        rows.append(
            {
                "companyName": item.get("Company Name", ""),
                "industry": item.get("Industry", ""),
                "symbol": item.get("Symbol", ""),
                "series": item.get("Series", ""),
                "isinCode": item.get("ISIN Code", ""),
            }
        )
    return rows