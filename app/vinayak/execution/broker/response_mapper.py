from __future__ import annotations

from vinayak.execution.broker.adapter_result import ExecutionAdapterResult


def map_dhan_response(
    route_result: dict[str, object],
    *,
    broker: str,
    fallback_status: str = 'PENDING_LIVE_ROUTE',
    fallback_price: float | None = None,
) -> ExecutionAdapterResult:
    raw_status = str(route_result.get('status') or route_result.get('orderStatus') or route_result.get('order_status') or fallback_status)
    normalized = raw_status.upper()
    if normalized in {'SUCCESS', 'TRANSIT', 'PENDING'}:
        normalized = 'ACCEPTED'
    elif normalized in {'REJECTED', 'CANCELLED', 'FAILED'}:
        normalized = 'REJECTED'

    broker_reference = route_result.get('broker_reference') or route_result.get('orderId') or route_result.get('order_id')

    return ExecutionAdapterResult(
        broker=broker,
        status=normalized,
        executed_price=fallback_price,
        executed_at=None,
        broker_reference=str(broker_reference) if broker_reference is not None else None,
        notes='Live adapter routed a Dhan order request using the configured broker API.',
        audit_request_payload=route_result.get('payload') if isinstance(route_result.get('payload'), dict) else None,
        audit_response_payload=route_result,
    )
