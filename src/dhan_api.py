from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib import error, request


def _clean(value: object) -> str:
    return str(value or "").strip()


def _normalize(value: object) -> str:
    return "".join(ch for ch in _clean(value).upper() if ch.isalnum())


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class DhanOrderRequest:
    security_id: str
    exchange_segment: str
    transaction_type: str
    quantity: int
    order_type: str = "MARKET"
    product_type: str = "INTRADAY"
    price: float = 0.0
    trigger_price: float = 0.0
    validity: str = "DAY"
    trading_symbol: str = ""
    tag: str = ""
    after_market_order: bool = False
    disclosed_quantity: int = 0
    bo_profit_value: float = 0.0
    bo_stop_loss_value: float = 0.0
    drv_expiry_date: str = ""
    drv_option_type: str = ""
    drv_strike_price: float = 0.0
    extra_payload: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "securityId": self.security_id,
            "exchangeSegment": self.exchange_segment,
            "transactionType": self.transaction_type,
            "quantity": int(self.quantity),
            "orderType": self.order_type,
            "productType": self.product_type,
            "price": float(self.price),
            "triggerPrice": float(self.trigger_price),
            "validity": self.validity,
            "tradingSymbol": self.trading_symbol,
            "tag": self.tag,
            "afterMarketOrder": bool(self.after_market_order),
            "disclosedQuantity": int(self.disclosed_quantity),
            "boProfitValue": float(self.bo_profit_value),
            "boStopLossValue": float(self.bo_stop_loss_value),
            "drvExpiryDate": self.drv_expiry_date,
            "drvOptionType": self.drv_option_type,
            "drvStrikePrice": float(self.drv_strike_price),
        }
        payload.update(self.extra_payload)
        return {key: value for key, value in payload.items() if value not in {"", None}}


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
        self.base_url = _clean(base_url or os.getenv("DHAN_BASE_URL", "https://api-hq.dhan.co")).rstrip("/")
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
            base_url=os.getenv("DHAN_BASE_URL", "https://api-hq.dhan.co"),
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
            raise RuntimeError(f"Dhan API error {exc.code}: {body}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Could not reach Dhan API: {exc.reason}") from exc

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

    def get_positions(self) -> Any:
        return self._request("GET", "/positions")


def _candidate_keys(candidate: dict[str, object]) -> list[str]:
    symbol = _clean(candidate.get("symbol"))
    option_strike = _clean(candidate.get("option_strike"))
    option_type = _clean(candidate.get("option_type"))
    trading_symbol = _clean(candidate.get("trading_symbol"))

    keys: list[str] = []
    for raw in (
        trading_symbol,
        option_strike,
        f"{symbol}{option_strike}",
        f"{symbol}{option_type}",
        symbol,
    ):
        normalized = _normalize(raw)
        if normalized and normalized not in keys:
            keys.append(normalized)
    return keys


def _aliases_for_row(row: dict[str, str]) -> list[str]:
    aliases: list[str] = []
    for key in (
        "alias",
        "aliases",
        "symbol",
        "underlying",
        "trading_symbol",
        "tradingSymbol",
        "contract_symbol",
        "contractSymbol",
        "instrument",
        "display_name",
    ):
        raw = _clean(row.get(key))
        if not raw:
            continue
        parts = [part.strip() for part in raw.replace("|", ",").split(",")]
        for part in parts:
            normalized = _normalize(part)
            if normalized and normalized not in aliases:
                aliases.append(normalized)
    return aliases


def load_security_map(path: Path) -> dict[str, dict[str, str]]:
    security_map: dict[str, dict[str, str]] = {}
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            row = {str(key).strip(): _clean(value) for key, value in raw_row.items() if key is not None}
            if not row:
                continue
            aliases = _aliases_for_row(row)
            security_id = row.get("security_id") or row.get("securityId") or row.get("sem_smst_security_id")
            if not aliases or not security_id:
                continue
            row.setdefault("security_id", security_id)
            row.setdefault("exchange_segment", row.get("exchangeSegment", row.get("exchange", row.get("exchange_segment", "NSE_FNO"))))
            row.setdefault("product_type", row.get("productType", row.get("product_type", "INTRADAY")))
            row.setdefault("order_type", row.get("orderType", row.get("order_type", "MARKET")))
            for alias in aliases:
                security_map[alias] = dict(row)
    return security_map


def _resolve_security(candidate: dict[str, object], security_map: dict[str, dict[str, str]] | None) -> dict[str, str]:
    if not security_map:
        raise ValueError("Security map is required for Dhan order creation")
    for key in _candidate_keys(candidate):
        if key in security_map:
            return dict(security_map[key])
    raise ValueError(f"No Dhan security map match found for symbol={_clean(candidate.get('symbol'))}")


def build_order_request_from_candidate(
    candidate: dict[str, object],
    *,
    client_id: str,
    security_map: dict[str, dict[str, str]] | None,
) -> DhanOrderRequest:
    del client_id

    security = _resolve_security(candidate, security_map)
    side = _clean(candidate.get("side")).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported side for live order: {side or 'EMPTY'}")

    quantity = _safe_int(candidate.get("quantity"), default=0)
    if quantity <= 0:
        raise ValueError("Quantity must be greater than zero")

    order_type = _clean(candidate.get("order_type") or security.get("order_type") or "MARKET").upper()
    price = _safe_float(candidate.get("price"), default=0.0)
    trigger_price = _safe_float(candidate.get("trigger_price"), default=0.0)

    tag_parts = [
        _clean(candidate.get("strategy")),
        _clean(candidate.get("symbol")),
        _clean(candidate.get("trade_no")),
    ]
    tag = "-".join(part for part in tag_parts if part)[:50]

    trading_symbol = (
        _clean(security.get("trading_symbol"))
        or _clean(security.get("tradingSymbol"))
        or _clean(security.get("contract_symbol"))
        or _clean(candidate.get("option_strike"))
        or _clean(candidate.get("symbol"))
    )

    drv_option_type = _clean(candidate.get("option_type") or security.get("option_type") or security.get("drv_option_type")).upper()
    strike_price = _safe_float(
        candidate.get("strike_price") or security.get("strike_price") or security.get("drv_strike_price"),
        default=0.0,
    )

    if order_type == "LIMIT" and price <= 0:
        raise ValueError("Limit orders require a positive price")
    if order_type == "STOP_LOSS" and trigger_price <= 0:
        raise ValueError("Stop-loss orders require a positive trigger_price")
    if order_type == "MARKET":
        price = 0.0

    return DhanOrderRequest(
        security_id=_clean(security.get("security_id")),
        exchange_segment=_clean(security.get("exchange_segment") or "NSE_FNO").upper(),
        transaction_type=side,
        quantity=quantity,
        order_type=order_type,
        product_type=_clean(candidate.get("product_type") or security.get("product_type") or "INTRADAY").upper(),
        price=price,
        trigger_price=trigger_price,
        validity=_clean(candidate.get("validity") or "DAY").upper(),
        trading_symbol=trading_symbol,
        tag=tag,
        after_market_order=str(candidate.get("after_market_order", "")).strip().lower() in {"1", "true", "yes", "on"},
        disclosed_quantity=_safe_int(candidate.get("disclosed_quantity"), default=0),
        bo_profit_value=_safe_float(candidate.get("bo_profit_value"), default=0.0),
        bo_stop_loss_value=_safe_float(candidate.get("bo_stop_loss_value"), default=0.0),
        drv_expiry_date=_clean(candidate.get("expiry_date") or security.get("expiry_date") or security.get("drv_expiry_date")),
        drv_option_type=drv_option_type,
        drv_strike_price=strike_price,
    )
