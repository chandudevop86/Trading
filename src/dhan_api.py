from __future__ import annotations

import csv
import json
import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib import error, request

DEFAULT_DHAN_API_URL = "https://api-hq.dhan.co"
DEFAULT_COMPACT_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
DEFAULT_DETAILED_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"

INVALID_SIDE = "INVALID_SIDE"
SYMBOL_NORMALIZATION_FAILED = "SYMBOL_NORMALIZATION_FAILED"
SECURITY_NOT_FOUND = "SECURITY_NOT_FOUND"
SECURITY_MAP_NOT_LOADED = "SECURITY_MAP_NOT_LOADED"
OPTION_RESOLUTION_FAILED = "OPTION_RESOLUTION_FAILED"
INVALID_EXPIRY = "INVALID_EXPIRY"
INVALID_STRIKE = "INVALID_STRIKE"
INVALID_ORDER_TYPE = "INVALID_ORDER_TYPE"
INVALID_QUANTITY = "INVALID_QUANTITY"
BROKER_CLIENT_NOT_CONFIGURED = "BROKER_CLIENT_NOT_CONFIGURED"
DHAN_API_ERROR = "DHAN_API_ERROR"

ORDER_TYPES = {"MARKET", "LIMIT", "STOP_LOSS", "STOP_LOSS_MARKET"}
PRODUCT_TYPES = {"CNC", "INTRADAY", "MARGIN", "MTF"}
VALIDITIES = {"DAY", "IOC"}
OPTION_TYPE_MAP = {"CE": "CALL", "CALL": "CALL", "PE": "PUT", "PUT": "PUT"}
OPTION_TYPE_SHORT = {"CALL": "CE", "PUT": "PE", "CE": "CE", "PE": "PE"}
YAHOO_SYMBOL_ALIASES = {
    "^NSEI": "NIFTY",
    "NSEI": "NIFTY",
    "NIFTY50": "NIFTY",
    "NIFTY 50": "NIFTY",
    "^NSEBANK": "BANKNIFTY",
    "NSEBANK": "BANKNIFTY",
    "NIFTYBANK": "BANKNIFTY",
    "BANK NIFTY": "BANKNIFTY",
}

_SECURITY_MAP_CACHE: dict[tuple[str, float], dict[str, Any]] = {}


@dataclass(slots=True)
class DhanExecutionError(ValueError):
    code: str
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        ValueError.__init__(self, self.message)

    def as_dict(self) -> dict[str, Any]:
        payload = {"code": self.code, "message": self.message}
        payload.update(self.metadata)
        return payload


@dataclass(slots=True)
class DhanOrderRequest:
    dhan_client_id: str
    security_id: str
    exchange_segment: str
    transaction_type: str
    quantity: int
    order_type: str = "MARKET"
    product_type: str = "INTRADAY"
    validity: str = "DAY"
    trading_symbol: str = ""
    correlation_id: str = ""
    price: float | None = None
    trigger_price: float | None = None
    after_market_order: bool = False
    amo_time: str = ""
    disclosed_quantity: int = 0
    drv_expiry_date: str = ""
    drv_option_type: str = ""
    drv_strike_price: float | None = None
    extra_payload: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "dhanClientId": self.dhan_client_id,
            "securityId": self.security_id,
            "exchangeSegment": self.exchange_segment,
            "transactionType": self.transaction_type,
            "quantity": int(self.quantity),
            "orderType": self.order_type,
            "productType": self.product_type,
            "validity": self.validity,
        }
        if self.trading_symbol:
            payload["tradingSymbol"] = self.trading_symbol
        if self.correlation_id:
            payload["correlationId"] = self.correlation_id
        if self.price is not None:
            payload["price"] = float(self.price)
        if self.trigger_price is not None:
            payload["triggerPrice"] = float(self.trigger_price)
        if self.after_market_order:
            payload["afterMarketOrder"] = True
        if self.amo_time:
            payload["amoTime"] = self.amo_time
        if self.disclosed_quantity > 0:
            payload["disclosedQuantity"] = int(self.disclosed_quantity)
        if self.drv_expiry_date:
            payload["drvExpiryDate"] = self.drv_expiry_date
        if self.drv_option_type:
            payload["drvOptionType"] = self.drv_option_type
        if self.drv_strike_price is not None:
            payload["drvStrikePrice"] = float(self.drv_strike_price)
        payload.update(self.extra_payload)
        return payload


class DhanClient:
    def __init__(
        self,
        client_id: str,
        access_token: str,
        *,
        base_url: str | None = None,
        timeout: int = 30,
    ) -> None:
        self.client_id = _clean(client_id)
        self.access_token = _clean(access_token)
        self.base_url = _clean(base_url or os.getenv("DHAN_BASE_URL", DEFAULT_DHAN_API_URL)).rstrip("/")
        self.timeout = int(timeout)
        if not self.client_id:
            raise ValueError("DHAN_CLIENT_ID is required")
        if not self.access_token:
            raise ValueError("DHAN_ACCESS_TOKEN is required")

    @classmethod
    def from_env(cls) -> "DhanClient":
        return cls(
            client_id=os.getenv("DHAN_CLIENT_ID", ""),
            access_token=os.getenv("DHAN_ACCESS_TOKEN", ""),
            base_url=os.getenv("DHAN_BASE_URL", DEFAULT_DHAN_API_URL),
            timeout=_safe_int(os.getenv("DHAN_TIMEOUT", "30"), default=30),
        )

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": self.access_token,
            "client-id": self.client_id,
        }

    def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = request.Request(url, data=data, headers=self._headers(), method=method.upper())
        try:
            with request.urlopen(req, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise DhanExecutionError(DHAN_API_ERROR, f"Dhan API error {exc.code}: {body}") from exc
        except error.URLError as exc:
            raise DhanExecutionError(DHAN_API_ERROR, f"Could not reach Dhan API: {exc.reason}") from exc

        if not raw.strip():
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"raw": raw}

    def place_order(self, order_request: DhanOrderRequest | dict[str, Any]) -> Any:
        payload = order_request.to_payload() if isinstance(order_request, DhanOrderRequest) else dict(order_request)
        return self._request("POST", "/orders", payload)

    def get_order_by_id(self, order_id: str) -> Any:
        return self._request("GET", f"/orders/{_clean(order_id)}")

    def get_order_by_correlation_id(self, correlation_id: str) -> Any:
        return self._request("GET", f"/orders/external/{_clean(correlation_id)}")

    def get_positions(self) -> Any:
        return self._request("GET", "/positions")

    def get_option_expiry_list(self, underlying_security_id: str | int, underlying_segment: str = "IDX_I") -> Any:
        return self._request(
            "POST",
            "/optionchain/expirylist",
            {"UnderlyingScrip": int(str(underlying_security_id)), "UnderlyingSeg": _clean(underlying_segment).upper()},
        )

    def get_option_chain(self, underlying_security_id: str | int, underlying_segment: str, expiry: str) -> Any:
        return self._request(
            "POST",
            "/optionchain",
            {
                "UnderlyingScrip": int(str(underlying_security_id)),
                "UnderlyingSeg": _clean(underlying_segment).upper(),
                "Expiry": normalize_expiry(expiry),
            },
        )

    def get_historical_data(
        self,
        *,
        security_id: str | int,
        exchange_segment: str,
        instrument: str,
        from_date: str,
        to_date: str,
        expiry_code: int = 0,
        oi: bool = False,
    ) -> Any:
        return self._request(
            "POST",
            "/charts/historical",
            {
                "securityId": str(security_id),
                "exchangeSegment": _clean(exchange_segment).upper(),
                "instrument": _clean(instrument).upper(),
                "expiryCode": int(expiry_code),
                "oi": bool(oi),
                "fromDate": _clean(from_date),
                "toDate": _clean(to_date),
            },
        )

    def get_intraday_data(
        self,
        *,
        security_id: str | int,
        exchange_segment: str,
        instrument: str,
        interval: int,
        from_date: str,
        to_date: str,
        oi: bool = False,
    ) -> Any:
        return self._request(
            "POST",
            "/charts/intraday",
            {
                "securityId": str(security_id),
                "exchangeSegment": _clean(exchange_segment).upper(),
                "instrument": _clean(instrument).upper(),
                "interval": int(interval),
                "oi": bool(oi),
                "fromDate": _clean(from_date),
                "toDate": _clean(to_date),
            },
        )


def _clean(value: object) -> str:
    return str(value or "").strip()


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_key(value: object) -> str:
    return "".join(ch for ch in _clean(value).upper() if ch.isalnum())


def _format_strike(value: object) -> str:
    number = _safe_float(value, default=float("nan"))
    if number != number:
        text = _clean(value)
        if not text:
            return ""
        return text.rstrip("0").rstrip(".") if "." in text else text
    if abs(number - round(number)) < 1e-9:
        return str(int(round(number)))
    return f"{number:.4f}".rstrip("0").rstrip(".")


def _sanitize_correlation_id(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9 _-]", "_", value or "")
    return cleaned[:30]


def _normalize_option_type(value: object) -> str:
    raw = _clean(value).upper()
    return OPTION_TYPE_SHORT.get(raw, "")


def normalize_expiry(value: object) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d %b %Y",
        "%d %B %Y",
        "%d-%b-%Y",
        "%d-%B-%Y",
    ):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return raw


def normalize_trading_symbol(symbol: str) -> str:
    raw = _clean(symbol)
    if not raw:
        return ""
    upper = raw.upper()
    if upper in YAHOO_SYMBOL_ALIASES:
        return YAHOO_SYMBOL_ALIASES[upper]
    compact = re.sub(r"[^A-Z0-9]", "", upper)
    if compact in YAHOO_SYMBOL_ALIASES:
        return YAHOO_SYMBOL_ALIASES[compact]
    return compact or upper


def build_option_symbol(underlying: str, expiry: str, strike: float | int | str, option_type: str) -> str:
    normalized_underlying = normalize_trading_symbol(underlying)
    normalized_expiry = normalize_expiry(expiry)
    normalized_option_type = _normalize_option_type(option_type)
    normalized_strike = _format_strike(strike)
    if not all((normalized_underlying, normalized_expiry, normalized_strike, normalized_option_type)):
        return ""
    return f"{normalized_underlying}-{normalized_expiry}-{normalized_strike}-{normalized_option_type}"


def _security_map_meta(security_map: dict[str, Any] | None) -> dict[str, Any]:
    if not security_map:
        return {}
    meta = security_map.get("__meta__") if isinstance(security_map, dict) else None
    return meta if isinstance(meta, dict) else {}


def _csv_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in row and _clean(row[key]):
            return _clean(row[key])
    upper_map = {str(k).upper(): v for k, v in row.items()}
    for key in keys:
        value = upper_map.get(key.upper())
        if _clean(value):
            return _clean(value)
    return ""


def _infer_underlying_symbol(underlying_symbol: str, symbol_name: str, display_name: str, trading_symbol: str, instrument_name: str) -> str:
    explicit = normalize_trading_symbol(underlying_symbol)
    if explicit:
        return explicit

    instrument_norm = _clean(instrument_name).upper()
    for raw in (trading_symbol, display_name, symbol_name):
        text = _clean(raw)
        if not text:
            continue
        if instrument_norm.startswith("OPT") or instrument_norm.startswith("FUT"):
            if "-" in text:
                return normalize_trading_symbol(text.split("-", 1)[0])
            parts = text.split()
            if parts:
                return normalize_trading_symbol(parts[0])
        normalized = normalize_trading_symbol(text)
        if normalized:
            return normalized
    return ""

def _infer_exchange_segment(exchange: str, segment: str, instrument_name: str, instrument_type: str, symbol_name: str) -> str:
    exchange_norm = _clean(exchange).upper()
    segment_norm = _clean(segment).upper()
    instrument_norm = _clean(instrument_name).upper()
    instrument_type_norm = _clean(instrument_type).upper()
    symbol_norm = normalize_trading_symbol(symbol_name)

    if instrument_norm in {"INDEX", "IDX", "INDEXES"} or instrument_type_norm in {"INDEX", "IDX_I"} or symbol_norm in {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"}:
        return "IDX_I"
    if exchange_norm == "NSE" and segment_norm == "D":
        return "NSE_FNO"
    if exchange_norm == "NSE" and segment_norm == "E":
        return "NSE_EQ"
    if exchange_norm == "BSE" and segment_norm == "D":
        return "BSE_FNO"
    if exchange_norm == "BSE" and segment_norm == "E":
        return "BSE_EQ"
    if exchange_norm == "MCX":
        return "MCX_COMM"
    return _clean(exchange_norm or segment_norm)


def _standardize_security_row(row: dict[str, str]) -> dict[str, Any]:
    security_id = _csv_value(row, "security_id", "securityId", "SEM_SMST_SECURITY_ID", "SM_SECURITY_ID", "SECURITY_ID")
    trading_symbol = _csv_value(row, "trading_symbol", "tradingSymbol", "SEM_TRADING_SYMBOL", "DISPLAY_NAME", "SEM_CUSTOM_SYMBOL")
    display_name = _csv_value(row, "display_name", "DISPLAY_NAME", "SEM_CUSTOM_SYMBOL") or trading_symbol
    symbol_name = _csv_value(row, "symbol", "symbol_name", "SM_SYMBOL_NAME", "SYMBOL_NAME") or display_name or trading_symbol
    exchange = _csv_value(row, "exchange", "EXCH_ID", "SEM_EXM_EXCH_ID")
    segment = _csv_value(row, "segment", "SEGMENT", "SEM_SEGMENT")
    instrument_name = _csv_value(row, "instrument", "INSTRUMENT", "SEM_INSTRUMENT_NAME")
    underlying_symbol = _infer_underlying_symbol(_csv_value(row, "underlying", "underlying_symbol", "UNDERLYING_SYMBOL"), symbol_name, display_name, trading_symbol, instrument_name)
    instrument_type = _csv_value(row, "instrument_type", "INSTRUMENT_TYPE", "SEM_EXCH_INSTRUMENT_TYPE") or instrument_name
    exchange_segment = _csv_value(row, "exchange_segment", "exchangeSegment") or _infer_exchange_segment(exchange, segment, instrument_name, instrument_type, underlying_symbol or symbol_name)
    expiry_date = normalize_expiry(_csv_value(row, "expiry_date", "SM_EXPIRY_DATE", "SEM_EXPIRY_DATE", "drv_expiry_date"))
    option_type = _normalize_option_type(_csv_value(row, "option_type", "OPTION_TYPE", "SEM_OPTION_TYPE", "drv_option_type"))
    strike_price = _format_strike(_csv_value(row, "strike_price", "STRIKE_PRICE", "SEM_STRIKE_PRICE", "drv_strike_price"))
    underlying_security_id = _csv_value(row, "underlying_security_id", "UNDERLYING_SECURITY_ID")
    product_type = _clean(_csv_value(row, "product_type", "productType") or "INTRADAY").upper()
    order_type = _clean(_csv_value(row, "order_type", "orderType") or "MARKET").upper()
    validity = _clean(_csv_value(row, "validity") or "DAY").upper()
    lot_size = _safe_int(_csv_value(row, "lot_size", "LOT_SIZE", "SEM_LOT_UNITS"), default=0)

    return {
        "security_id": security_id,
        "securityId": security_id,
        "trading_symbol": trading_symbol or display_name or symbol_name,
        "tradingSymbol": trading_symbol or display_name or symbol_name,
        "display_name": display_name,
        "symbol_name": symbol_name,
        "underlying_symbol": normalize_trading_symbol(underlying_symbol or symbol_name),
        "underlying_security_id": underlying_security_id,
        "exchange": exchange.upper(),
        "segment": segment.upper(),
        "exchange_segment": exchange_segment.upper(),
        "exchangeSegment": exchange_segment.upper(),
        "instrument_name": instrument_name.upper(),
        "instrument_type": instrument_type.upper(),
        "series": _csv_value(row, "series", "SERIES", "SEM_SERIES").upper(),
        "lot_size": lot_size,
        "tick_size": _csv_value(row, "tick_size", "TICK_SIZE", "SEM_TICK_SIZE"),
        "expiry_date": expiry_date,
        "drv_expiry_date": expiry_date,
        "strike_price": strike_price,
        "drv_strike_price": strike_price,
        "option_type": option_type,
        "drv_option_type": OPTION_TYPE_MAP.get(option_type, ""),
        "product_type": product_type if product_type in PRODUCT_TYPES else "INTRADAY",
        "order_type": order_type if order_type in ORDER_TYPES else "MARKET",
        "validity": validity if validity in VALIDITIES else "DAY",
        "raw_row": dict(row),
    }


def _instrument_aliases(record: dict[str, Any]) -> list[str]:
    aliases: list[str] = []
    candidates = [
        record.get("trading_symbol"),
        record.get("display_name"),
        record.get("symbol_name"),
        record.get("underlying_symbol"),
        record.get("security_id"),
    ]
    option_key = build_option_symbol(
        str(record.get("underlying_symbol", "")),
        str(record.get("expiry_date", "")),
        str(record.get("strike_price", "")),
        str(record.get("option_type", "")),
    )
    if option_key:
        candidates.append(option_key)
        candidates.append(f"{record.get('underlying_symbol', '')}{record.get('strike_price', '')}{record.get('option_type', '')}")
    for raw in candidates:
        cleaned = _clean(raw)
        if cleaned and cleaned not in aliases:
            aliases.append(cleaned)
        normalized = _normalize_key(raw)
        if normalized and normalized not in aliases:
            aliases.append(normalized)
    return aliases

def _build_security_map(records: list[dict[str, Any]], *, source_path: str, detailed_path: str | None = None) -> dict[str, Any]:
    security_map: dict[str, Any] = {}
    by_security_id: dict[str, dict[str, Any]] = {}
    by_trading_symbol: dict[str, dict[str, Any]] = {}
    by_underlying: dict[str, list[dict[str, Any]]] = {}
    by_exchange_segment: dict[str, list[dict[str, Any]]] = {}
    by_instrument_type: dict[str, list[dict[str, Any]]] = {}
    option_index: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for record in records:
        security_id = _clean(record.get("security_id"))
        if not security_id:
            continue
        by_security_id[security_id] = record
        trading_key = _normalize_key(record.get("trading_symbol"))
        if trading_key:
            by_trading_symbol[trading_key] = record
        underlying_key = normalize_trading_symbol(str(record.get("underlying_symbol", "")))
        if underlying_key:
            by_underlying.setdefault(underlying_key, []).append(record)
        exchange_segment = _clean(record.get("exchange_segment")).upper()
        if exchange_segment:
            by_exchange_segment.setdefault(exchange_segment, []).append(record)
        instrument_type = _clean(record.get("instrument_type")).upper()
        if instrument_type:
            by_instrument_type.setdefault(instrument_type, []).append(record)
        option_type = _normalize_option_type(record.get("option_type"))
        expiry_date = normalize_expiry(record.get("expiry_date"))
        strike_price = _format_strike(record.get("strike_price"))
        if underlying_key and expiry_date and strike_price and option_type:
            option_index[(underlying_key, expiry_date, strike_price, option_type)] = record
        for alias in _instrument_aliases(record):
            security_map.setdefault(alias, record)

    security_map["__meta__"] = {
        "source_path": source_path,
        "detailed_path": detailed_path or "",
        "loaded_at_utc": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
        "records": records,
        "by_security_id": by_security_id,
        "by_trading_symbol": by_trading_symbol,
        "by_underlying": by_underlying,
        "by_exchange_segment": by_exchange_segment,
        "by_instrument_type": by_instrument_type,
        "option_index": option_index,
    }
    return security_map


def load_security_map(csv_path: str | Path | None = None, detailed_csv_path: str | Path | None = None) -> dict[str, Any]:
    compact_path = Path(csv_path or os.getenv("DHAN_SECURITY_MAP", "data/dhan_security_map.csv"))
    if not compact_path.exists():
        raise FileNotFoundError(f"Dhan security map CSV not found: {compact_path}")
    cache_key = (str(compact_path.resolve()), compact_path.stat().st_mtime)
    cached = _SECURITY_MAP_CACHE.get(cache_key)
    if cached is not None:
        return cached

    records: list[dict[str, Any]] = []
    with compact_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            row = {str(key).strip(): _clean(value) for key, value in raw_row.items() if key is not None}
            record = _standardize_security_row(row)
            if _clean(record.get("security_id")):
                records.append(record)

    detailed_path_obj = Path(detailed_csv_path) if detailed_csv_path else None
    if detailed_path_obj is not None and detailed_path_obj.exists():
        with detailed_path_obj.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            detailed_by_security: dict[str, dict[str, str]] = {}
            for raw_row in reader:
                row = {str(key).strip(): _clean(value) for key, value in raw_row.items() if key is not None}
                security_id = _csv_value(row, "security_id", "securityId", "SEM_SMST_SECURITY_ID", "SM_SECURITY_ID", "SECURITY_ID")
                if security_id:
                    detailed_by_security[security_id] = row
            for record in records:
                extra = detailed_by_security.get(str(record.get("security_id", "")))
                if extra:
                    record["raw_row"] = {**record.get("raw_row", {}), **extra}

    built = _build_security_map(records, source_path=str(compact_path), detailed_path=str(detailed_path_obj) if detailed_path_obj else None)
    _SECURITY_MAP_CACHE.clear()
    _SECURITY_MAP_CACHE[cache_key] = built
    return built


def refresh_security_map_from_dhan(
    csv_path: str | Path | None = None,
    *,
    detailed_csv_path: str | Path | None = None,
    compact_url: str = DEFAULT_COMPACT_SCRIP_MASTER_URL,
    detailed_url: str = DEFAULT_DETAILED_SCRIP_MASTER_URL,
    timeout: int = 60,
) -> dict[str, Any]:
    compact_target = Path(csv_path or os.getenv("DHAN_SECURITY_MAP", "data/dhan_security_map.csv"))
    compact_target.parent.mkdir(parents=True, exist_ok=True)
    _download_file(compact_url, compact_target, timeout=timeout)

    detailed_target: Path | None = None
    if detailed_csv_path is not None:
        detailed_target = Path(detailed_csv_path)
        detailed_target.parent.mkdir(parents=True, exist_ok=True)
        _download_file(detailed_url, detailed_target, timeout=timeout)

    return load_security_map(compact_target, detailed_target)


def _download_file(url: str, destination: Path, *, timeout: int) -> None:
    req = request.Request(url, headers={"User-Agent": "TradingBot/1.0"})
    try:
        with request.urlopen(req, timeout=timeout) as response:
            data = response.read()
    except error.HTTPError as exc:
        raise DhanExecutionError(DHAN_API_ERROR, f"Failed to download Dhan instrument master: HTTP {exc.code}") from exc
    except error.URLError as exc:
        raise DhanExecutionError(DHAN_API_ERROR, f"Failed to download Dhan instrument master: {exc.reason}") from exc
    destination.write_bytes(data)


def find_cash_instrument(security_map: dict[str, Any] | None, symbol: str, *, exchange_segment: str | None = None, instrument_type: str | None = None) -> dict[str, Any] | None:
    if not security_map:
        return None
    normalized_symbol = normalize_trading_symbol(symbol)
    meta = _security_map_meta(security_map)
    candidates = list(meta.get("by_underlying", {}).get(normalized_symbol, []))
    if not candidates:
        direct = security_map.get(normalized_symbol) or security_map.get(_normalize_key(normalized_symbol))
        if isinstance(direct, dict):
            candidates = [direct]
    if not candidates:
        return None
    exchange_filter = _clean(exchange_segment).upper()
    instrument_filter = _clean(instrument_type).upper()
    filtered: list[dict[str, Any]] = []
    for record in candidates:
        if _normalize_option_type(record.get("option_type")):
            continue
        if exchange_filter and _clean(record.get("exchange_segment")).upper() != exchange_filter:
            continue
        if instrument_filter and instrument_filter not in _clean(record.get("instrument_type")).upper():
            continue
        filtered.append(record)
    if filtered:
        return filtered[0]
    return candidates[0]


def find_option_instrument(security_map: dict[str, Any] | None, underlying: str, expiry: str, strike: float | int | str, option_type: str, *, exchange_segment: str | None = "NSE_FNO") -> dict[str, Any] | None:
    if not security_map:
        return None
    meta = _security_map_meta(security_map)
    option_index = meta.get("option_index", {}) if meta else {}
    key = (normalize_trading_symbol(underlying), normalize_expiry(expiry), _format_strike(strike), _normalize_option_type(option_type))
    record = option_index.get(key)
    if isinstance(record, dict):
        if exchange_segment and _clean(record.get("exchange_segment")).upper() != _clean(exchange_segment).upper():
            return None
        return record
    return None


def infer_option_expiry(security_map: dict[str, Any] | None, underlying: str, strike: float | int | str, option_type: str, *, exchange_segment: str | None = "NSE_FNO") -> str:
    if not security_map:
        return ""
    meta = _security_map_meta(security_map)
    candidates = list(meta.get("by_underlying", {}).get(normalize_trading_symbol(underlying), []))
    if not candidates:
        return ""

    strike_key = _format_strike(strike)
    option_key = _normalize_option_type(option_type)
    segment_key = _clean(exchange_segment).upper()
    expiries: list[str] = []
    for record in candidates:
        if strike_key and _format_strike(record.get("strike_price")) != strike_key:
            continue
        if option_key and _normalize_option_type(record.get("option_type")) != option_key:
            continue
        if segment_key and _clean(record.get("exchange_segment")).upper() != segment_key:
            continue
        expiry = normalize_expiry(record.get("expiry_date"))
        if expiry:
            expiries.append(expiry)
    return min(expiries) if expiries else ""


def _extract_option_contract(candidate: dict[str, object]) -> tuple[str, str]:
    option_type = _normalize_option_type(candidate.get("option_type") or candidate.get("drv_option_type"))
    strike = _format_strike(candidate.get("strike_price") or candidate.get("drv_strike_price"))
    option_strike = _clean(candidate.get("option_strike"))
    if (not strike or not option_type) and option_strike:
        match = re.search(r"(\d+(?:\.\d+)?)\s*(CE|PE|CALL|PUT)$", option_strike.upper())
        if match:
            strike = strike or _format_strike(match.group(1))
            option_type = option_type or _normalize_option_type(match.group(2))
    return strike, option_type


def _option_chain_validation(broker_client: object | None, *, underlying_security_id: str, underlying_segment: str, expiry: str, strike: str, option_type: str) -> None:
    if broker_client is None or not hasattr(broker_client, "get_option_expiry_list"):
        return
    expiries_raw = broker_client.get_option_expiry_list(underlying_security_id, underlying_segment)
    expiries = expiries_raw.get("data", []) if isinstance(expiries_raw, dict) else []
    normalized_expiries = {normalize_expiry(item) for item in expiries}
    if normalized_expiries and normalize_expiry(expiry) not in normalized_expiries:
        raise DhanExecutionError(INVALID_EXPIRY, f"Expiry {normalize_expiry(expiry)} is not available for {underlying_segment}:{underlying_security_id}")
    if not hasattr(broker_client, "get_option_chain"):
        return
    chain_raw = broker_client.get_option_chain(underlying_security_id, underlying_segment, normalize_expiry(expiry))
    chain = chain_raw.get("data", {}).get("oc", {}) if isinstance(chain_raw, dict) else {}
    strike_entry = chain.get(f"{float(strike):.6f}") or chain.get(strike)
    if not strike_entry:
        raise DhanExecutionError(INVALID_STRIKE, f"Strike {strike} is not available for expiry {normalize_expiry(expiry)}")
    leg = strike_entry.get(option_type.lower()) if isinstance(strike_entry, dict) else None
    if not leg:
        raise DhanExecutionError(OPTION_RESOLUTION_FAILED, f"Option type {option_type} is not available for strike {strike}")


def resolve_security(candidate: dict[str, object], security_map: dict[str, Any] | None, *, broker_client: object | None = None, validate_with_option_chain: bool = False) -> dict[str, Any]:
    if not security_map:
        raise DhanExecutionError(SECURITY_MAP_NOT_LOADED, "Dhan security map is not loaded")

    data_symbol = _clean(candidate.get("data_symbol") or candidate.get("symbol"))
    trade_symbol = _clean(candidate.get("trade_symbol") or candidate.get("trading_symbol") or normalize_trading_symbol(data_symbol))
    if not trade_symbol:
        raise DhanExecutionError(SYMBOL_NORMALIZATION_FAILED, f"Could not normalize trading symbol from {data_symbol or 'EMPTY'}")

    security_id = _clean(candidate.get("security_id") or candidate.get("securityId"))
    meta = _security_map_meta(security_map)
    if security_id and security_id in meta.get("by_security_id", {}):
        record = dict(meta["by_security_id"][security_id])
    else:
        strike, option_type = _extract_option_contract(candidate)
        expiry = normalize_expiry(candidate.get("option_expiry") or candidate.get("expiry") or candidate.get("expiry_date") or candidate.get("drv_expiry_date"))
        is_option_request = bool(strike or option_type or expiry or _clean(candidate.get("option_strike")))
        if is_option_request:
            if not strike:
                raise DhanExecutionError(INVALID_STRIKE, f"Option strike is required for {trade_symbol}")
            if not option_type:
                raise DhanExecutionError(OPTION_RESOLUTION_FAILED, f"Option type is required for {trade_symbol}")
            exchange_segment = _clean(candidate.get("exchange_segment") or "NSE_FNO") or "NSE_FNO"
            if not expiry:
                expiry = infer_option_expiry(security_map, trade_symbol, strike, option_type, exchange_segment=exchange_segment)
            if not expiry:
                raise DhanExecutionError(INVALID_EXPIRY, f"Option expiry is required for {trade_symbol}")
            record = find_option_instrument(security_map, trade_symbol, expiry, strike, option_type, exchange_segment=exchange_segment)
            if record is None and validate_with_option_chain:
                underlying_record = find_cash_instrument(security_map, trade_symbol)
                if underlying_record and _clean(underlying_record.get("security_id")):
                    _option_chain_validation(
                        broker_client,
                        underlying_security_id=str(underlying_record.get("security_id")),
                        underlying_segment=str(underlying_record.get("exchange_segment") or "IDX_I"),
                        expiry=expiry,
                        strike=strike,
                        option_type=option_type,
                    )
            if record is None:
                raise DhanExecutionError(
                    OPTION_RESOLUTION_FAILED,
                    f"No Dhan option instrument found for {trade_symbol} {expiry} {strike}{option_type}",
                    metadata={
                        "data_symbol": data_symbol,
                        "trade_symbol": trade_symbol,
                        "option_expiry": expiry,
                        "strike_price": strike,
                        "option_type": option_type,
                    },
                )
        else:
            direct = security_map.get(trade_symbol) or security_map.get(_normalize_key(trade_symbol))
            record = dict(direct) if isinstance(direct, dict) else {}
            if not record:
                found = find_cash_instrument(security_map, trade_symbol, exchange_segment=_clean(candidate.get("exchange_segment")), instrument_type=_clean(candidate.get("instrument_type")))
                record = dict(found) if isinstance(found, dict) else {}
            if not record:
                raise DhanExecutionError(
                    SECURITY_NOT_FOUND,
                    f"No Dhan security map match found for symbol={data_symbol or trade_symbol}",
                    metadata={"data_symbol": data_symbol, "trade_symbol": trade_symbol},
                )

    record["data_symbol"] = data_symbol
    record["trade_symbol"] = trade_symbol
    record["security_id"] = _clean(record.get("security_id") or record.get("securityId"))
    record["exchange_segment"] = _clean(record.get("exchange_segment") or record.get("exchangeSegment")).upper()
    record["instrument_type"] = _clean(record.get("instrument_type")).upper()
    record["option_expiry"] = normalize_expiry(candidate.get("option_expiry") or candidate.get("expiry") or candidate.get("expiry_date") or candidate.get("drv_expiry_date") or record.get("expiry_date"))
    record["option_type"] = _normalize_option_type(candidate.get("option_type") or candidate.get("drv_option_type") or record.get("option_type"))
    record["strike_price"] = _format_strike(candidate.get("strike_price") or candidate.get("drv_strike_price") or record.get("strike_price"))
    return record


def build_order_request_from_candidate(candidate: dict[str, object], *, client_id: str, security_map: dict[str, Any] | None, resolved_security: dict[str, Any] | None = None, broker_client: object | None = None, validate_with_option_chain: bool = False) -> DhanOrderRequest:
    if not client_id:
        raise DhanExecutionError(BROKER_CLIENT_NOT_CONFIGURED, "Dhan client id is required for order placement")

    resolution = dict(resolved_security or resolve_security(candidate, security_map, broker_client=broker_client, validate_with_option_chain=validate_with_option_chain))
    side = _clean(candidate.get("side")).upper()
    if side not in {"BUY", "SELL"}:
        raise DhanExecutionError(INVALID_SIDE, f"Unsupported side for Dhan order: {side or 'EMPTY'}")

    quantity = _safe_int(candidate.get("quantity"), default=0)
    if quantity <= 0:
        raise DhanExecutionError(INVALID_QUANTITY, "Quantity must be greater than zero")

    order_type = _clean(candidate.get("order_type") or resolution.get("order_type") or "MARKET").upper()
    if order_type not in ORDER_TYPES:
        raise DhanExecutionError(INVALID_ORDER_TYPE, f"Unsupported Dhan order type: {order_type or 'EMPTY'}")

    product_type = _clean(candidate.get("product_type") or resolution.get("product_type") or "INTRADAY").upper()
    if product_type not in PRODUCT_TYPES:
        product_type = "INTRADAY"
    validity = _clean(candidate.get("validity") or resolution.get("validity") or "DAY").upper()
    if validity not in VALIDITIES:
        validity = "DAY"

    price = _safe_float(candidate.get("limit_price") or candidate.get("price"), default=0.0)
    trigger_price = _safe_float(candidate.get("trigger_price"), default=0.0)
    if order_type == "MARKET":
        payload_price: float | None = None
        payload_trigger: float | None = None
    elif order_type == "LIMIT":
        if price <= 0:
            raise DhanExecutionError(INVALID_ORDER_TYPE, "LIMIT orders require a positive price")
        payload_price = price
        payload_trigger = None
    elif order_type == "STOP_LOSS":
        if price <= 0 or trigger_price <= 0:
            raise DhanExecutionError(INVALID_ORDER_TYPE, "STOP_LOSS orders require positive price and trigger_price")
        payload_price = price
        payload_trigger = trigger_price
    else:
        if trigger_price <= 0:
            raise DhanExecutionError(INVALID_ORDER_TYPE, "STOP_LOSS_MARKET orders require a positive trigger_price")
        payload_price = None
        payload_trigger = trigger_price

    correlation_seed = _clean(candidate.get("correlation_id") or candidate.get("trade_id") or f"{candidate.get('strategy','')}-{resolution.get('trade_symbol','')}-{candidate.get('trade_no','')}")
    correlation_id = _sanitize_correlation_id(correlation_seed)
    option_type = _normalize_option_type(candidate.get("option_type") or candidate.get("drv_option_type") or resolution.get("option_type"))
    strike_price = _format_strike(candidate.get("strike_price") or candidate.get("drv_strike_price") or resolution.get("strike_price"))
    expiry = normalize_expiry(candidate.get("option_expiry") or candidate.get("expiry") or candidate.get("expiry_date") or candidate.get("drv_expiry_date") or resolution.get("expiry_date"))

    return DhanOrderRequest(
        dhan_client_id=client_id,
        security_id=_clean(resolution.get("security_id")),
        exchange_segment=_clean(candidate.get("exchange_segment") or resolution.get("exchange_segment") or "NSE_FNO").upper(),
        transaction_type=side,
        quantity=quantity,
        order_type=order_type,
        product_type=product_type,
        validity=validity,
        trading_symbol=_clean(resolution.get("trading_symbol") or resolution.get("display_name") or resolution.get("trade_symbol")),
        correlation_id=correlation_id,
        price=payload_price,
        trigger_price=payload_trigger,
        after_market_order=str(candidate.get("after_market_order", "")).strip().lower() in {"1", "true", "yes", "on"},
        amo_time=_clean(candidate.get("amo_time")).upper(),
        disclosed_quantity=_safe_int(candidate.get("disclosed_quantity"), default=0),
        drv_expiry_date=expiry,
        drv_option_type=OPTION_TYPE_MAP.get(option_type, ""),
        drv_strike_price=float(strike_price) if strike_price else None,
    )







