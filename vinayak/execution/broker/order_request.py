from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class DhanOrderRequest:
    security_id: str
    exchange_segment: str
    transaction_type: str
    quantity: int
    order_type: str = 'MARKET'
    product_type: str = 'INTRADAY'
    price: float = 0.0
    trigger_price: float = 0.0
    validity: str = 'DAY'
    trading_symbol: str = ''
    tag: str = ''
    after_market_order: bool = False
    disclosed_quantity: int = 0
    bo_profit_value: float = 0.0
    bo_stop_loss_value: float = 0.0
    drv_expiry_date: str = ''
    drv_option_type: str = ''
    drv_strike_price: float = 0.0
    metadata: dict[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            'securityId': self.security_id,
            'exchangeSegment': self.exchange_segment,
            'transactionType': self.transaction_type,
            'quantity': int(self.quantity),
            'orderType': self.order_type,
            'productType': self.product_type,
            'price': float(self.price),
            'triggerPrice': float(self.trigger_price),
            'validity': self.validity,
            'tradingSymbol': self.trading_symbol,
            'tag': self.tag,
            'afterMarketOrder': bool(self.after_market_order),
            'disclosedQuantity': int(self.disclosed_quantity),
            'boProfitValue': float(self.bo_profit_value),
            'boStopLossValue': float(self.bo_stop_loss_value),
            'drvExpiryDate': self.drv_expiry_date,
            'drvOptionType': self.drv_option_type,
            'drvStrikePrice': float(self.drv_strike_price),
        }
        if self.metadata:
            payload['metadata'] = self.metadata
        return {key: value for key, value in payload.items() if value is not None and value != ''}

