from datetime import UTC, datetime

from vinayak.db.models.reviewed_trade import ReviewedTradeRecord
from vinayak.db.models.signal import SignalRecord
from vinayak.execution.broker.order_request import DhanOrderRequest
from vinayak.execution.broker.payload_builder import build_dhan_order_request


def test_build_dhan_order_request_from_reviewed_trade(tmp_path) -> None:
    security_map = {
        '^NSEI': {
            'security_id': 'IDXNIFTY',
            'exchange_segment': 'NSE_FNO',
            'product_type': 'INTRADAY',
            'order_type': 'MARKET',
            'trading_symbol': 'NIFTY 50',
        }
    }
    reviewed_trade = ReviewedTradeRecord(
        id=11,
        signal_id=22,
        strategy_name='Breakout',
        symbol='^NSEI',
        side='BUY',
        entry_price=23150.0,
        stop_loss=23100.0,
        target_price=23250.0,
        quantity=50,
        lots=1,
        status='APPROVED',
        notes='Approved trade',
        created_at=datetime.now(UTC),
    )

    order_request = build_dhan_order_request(reviewed_trade=reviewed_trade, signal=None, security_map=security_map)

    assert isinstance(order_request, DhanOrderRequest)
    assert order_request.security_id == 'IDXNIFTY'
    assert order_request.transaction_type == 'BUY'
    assert order_request.quantity == 50
    payload = order_request.to_payload()
    assert payload['securityId'] == 'IDXNIFTY'
    assert payload['metadata']['reviewed_trade_id'] == 11
    assert payload['metadata']['signal_id'] == 22


def test_build_dhan_order_request_from_signal_fallback() -> None:
    security_map = {
        '^NSEI': {
            'security_id': 'IDXNIFTY',
            'exchange_segment': 'NSE_FNO',
            'product_type': 'INTRADAY',
            'order_type': 'MARKET',
            'trading_symbol': 'NIFTY 50',
        }
    }
    signal = SignalRecord(
        id=9,
        strategy_name='Indicator',
        symbol='^NSEI',
        side='SELL',
        entry_price=23080.0,
        stop_loss=23120.0,
        target_price=23000.0,
        signal_time=datetime.fromisoformat('2026-03-20T09:15:00'),
        status='NEW',
    )

    order_request = build_dhan_order_request(reviewed_trade=None, signal=signal, security_map=security_map)

    assert order_request.transaction_type == 'SELL'
    assert order_request.quantity == 1
    assert order_request.to_payload()['metadata']['signal_id'] == 9
