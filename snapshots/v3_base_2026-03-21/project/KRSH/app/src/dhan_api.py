from __future__ import annotations

import csv
import json
import os
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_BASE_URL = "https://api.dhan.co/v2"
DEFAULT_SECURITY_MAP_PATH = Path("data/dhan_security_map.csv")


@dataclass
class DhanOrderRequest:
    dhanClientId: str
    correlationId: str
    transactionType: str
    exchangeSegment: str
    productType: str
    orderType: str
    validity: str
    securityId: str
    quantity: int
    price: float = 0.0
    triggerPrice: float = 0.0
    disclosedQuantity: int = 0
    afterMarketOrder: bool = False
    amoTime: str = "OPEN"
    tradingSymbol: str = ""
    drvExpiryDate: str = ""
    drvOptionType: str = ""
    drvStrikePrice: float = 0.0

    def to_payload(self) -> dict[str, Any]:
        return {
            "dhanClientId": self.dhanClientId,
            "correlationId": self.correlationId,
            "transactionType": self.transactionType,
            "exchangeSegment": self.exchangeSegment,
            "productType": self.productType,
            "orderType": self.orderType,
            "validity": self.validity,
            "tradingSymbol": self.tradingSymbol,
            "securityId": self.securityId,
            "quantity": int(self.quantity),
            "disclosedQuantity": int(self.disclosedQuantity),
            "price": float(self.price),
            "triggerPrice": float(self.triggerPrice),
            "afterMarketOrder": bool(self.afterMarketOrder),
            "amoTime": self.amoTime,
            "drvExpiryDate": self.drvExpiryDate or "",
            "drvOptionType": self.drvOptionType or "",
            "drvStrikePrice": float(self.drvStrikePrice or 0.0),
        }


class DhanClient:
    def __init__(self, client_id: str, access_token: str, base_url: str = DEFAULT_BASE_URL) -> None:
        self.client_id = str(client_id or "").strip()
        self.access_token = str(access_token or "").strip()
        self.base_url = base_url.rstrip("/")
        if not self.client_id or not self.access_token:
            raise ValueError("Dhan client_id and access_token are required")

    @classmethod
    def from_env(cls) -> "DhanClient | None":
        client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        access_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if not client_id or not access_token:
            return None
        base_url = os.getenv("DHAN_BASE_URL", DEFAULT_BASE_URL).strip() or DEFAULT_BASE_URL
        return cls(client_id=client_id, access_token=access_token, base_url=base_url)

    def place_order(self, request: DhanOrderRequest | dict[str, Any]) -> dict[str, Any]:
        payload = request.to_payload() if isinstance(request, DhanOrderRequest) else dict(request)
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=f"{self.base_url}/orders",
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "access-token": self.access_token,
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                detail = json.loads(raw)
            except Exception:
                detail = {"message": raw}
            raise RuntimeError(f"Dhan order placement failed: HTTP {exc.code} {detail}") from exc


def load_security_map(path: Path | None = None) -> dict[str, dict[str, str]]:
    map_path = path or DEFAULT_SECURITY_MAP_PATH
    if not map_path.exists():
        return {}

    out: dict[str, dict[str, str]] = {}
    with map_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            normalized = {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}
            for key_field in ("symbol", "trading_symbol", "option_strike", "contract_symbol", "security_id"):
                key = normalized.get(key_field, "").upper()
                if key:
                    out[key] = normalized
    return out


def _pick_mapping(candidate: dict[str, Any], security_map: dict[str, dict[str, str]]) -> dict[str, str]:
    if not security_map:
        return {}

    candidate_keys = [
        str(candidate.get("security_id", "")).strip().upper(),
        str(candidate.get("trading_symbol", "")).strip().upper(),
        str(candidate.get("contract_symbol", "")).strip().upper(),
        str(candidate.get("option_strike", "")).strip().upper(),
        str(candidate.get("symbol", "")).strip().upper(),
    ]
    for key in candidate_keys:
        if key and key in security_map:
            return security_map[key]
    return {}


def build_order_request_from_candidate(
    candidate: dict[str, Any],
    *,
    client_id: str,
    security_map: dict[str, dict[str, str]] | None = None,
    exchange_segment: str | None = None,
    product_type: str | None = None,
    order_type: str | None = None,
    validity: str | None = None,
) -> DhanOrderRequest:
    side = str(candidate.get("side", "")).strip().upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported side for live order: {side}")

    mapping = _pick_mapping(candidate, security_map or {})
    security_id = str(candidate.get("security_id") or candidate.get("securityId") or mapping.get("security_id") or mapping.get("securityId") or "").strip()
    if not security_id:
        raise ValueError("security_id is required for Dhan live order placement")

    trading_symbol = str(candidate.get("trading_symbol") or mapping.get("trading_symbol") or mapping.get("symbol") or candidate.get("option_strike") or candidate.get("contract_symbol") or candidate.get("symbol") or "").strip()
    instrument = str(candidate.get("instrument") or mapping.get("instrument") or "").strip().upper()
    option_type = str(candidate.get("option_type") or mapping.get("option_type") or "").strip().upper()
    strike_price = candidate.get("strike_price") or mapping.get("strike_price") or 0
    expiry = str(candidate.get("contract_expiry") or candidate.get("option_expiry") or mapping.get("expiry_date") or "").strip()

    inferred_segment = "NSE_FNO" if instrument in {"FUTURES", "OPTIONS", "FUTIDX", "OPTIDX"} or option_type in {"CE", "PE"} else "NSE_EQ"
    seg = str(exchange_segment or candidate.get("exchange_segment") or mapping.get("exchange_segment") or os.getenv("DHAN_EXCHANGE_SEGMENT", inferred_segment)).strip() or inferred_segment
    prod = str(product_type or candidate.get("product_type") or mapping.get("product_type") or os.getenv("DHAN_PRODUCT_TYPE", "INTRADAY")).strip() or "INTRADAY"
    otype = str(order_type or candidate.get("order_type") or mapping.get("order_type") or os.getenv("DHAN_ORDER_TYPE", "MARKET")).strip() or "MARKET"
    valid = str(validity or candidate.get("validity") or mapping.get("validity") or os.getenv("DHAN_VALIDITY", "DAY")).strip() or "DAY"

    quantity = int(float(candidate.get("quantity") or mapping.get("quantity") or 0))
    if quantity <= 0:
        raise ValueError("quantity must be greater than zero for live order placement")

    price = float(candidate.get("limit_price") or candidate.get("price") or 0.0)
    trigger_price = float(candidate.get("trigger_price") or 0.0)

    drv_option_type = ""
    if option_type == "CE":
        drv_option_type = "CALL"
    elif option_type == "PE":
        drv_option_type = "PUT"

    correlation_id = str(candidate.get("correlation_id") or uuid.uuid4().hex[:20])

    return DhanOrderRequest(
        dhanClientId=client_id,
        correlationId=correlation_id,
        transactionType=side,
        exchangeSegment=seg,
        productType=prod,
        orderType=otype,
        validity=valid,
        securityId=security_id,
        quantity=quantity,
        price=0.0 if otype == "MARKET" else price,
        triggerPrice=trigger_price,
        tradingSymbol=trading_symbol,
        drvExpiryDate=expiry,
        drvOptionType=drv_option_type,
        drvStrikePrice=float(strike_price or 0.0),
    )
