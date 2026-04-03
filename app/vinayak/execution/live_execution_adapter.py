from __future__ import annotations

import os
from datetime import UTC, datetime

from vinayak.execution.broker.adapter_result import ExecutionAdapterResult
from vinayak.execution.broker.dhan_client import (
    DhanClient,
    DhanClientConfigError,
    DhanClientRequestError,
)
from vinayak.execution.broker.payload_builder import build_dhan_order_request
from vinayak.execution.broker.response_mapper import map_dhan_response


class LiveExecutionAdapter:
    def execute(self, *, command, reviewed_trade=None, signal=None) -> ExecutionAdapterResult:
        broker = str(command.broker or "").upper().strip()
        if broker != "DHAN":
            return ExecutionAdapterResult(
                broker=broker or "LIVE",
                status="BLOCKED",
                executed_price=command.executed_price,
                executed_at=datetime.now(UTC),
                notes=f"Live execution is not configured for broker {command.broker}.",
                audit_request_payload={"broker": broker or command.broker},
                audit_response_payload={"error": f"unsupported_broker:{command.broker}"},
            )

        client = DhanClient(
            client_id=os.getenv("DHAN_CLIENT_ID"),
            access_token=os.getenv("DHAN_ACCESS_TOKEN"),
        )

        try:
            order_request = build_dhan_order_request(
                reviewed_trade=reviewed_trade,
                signal=signal,
                fallback_price=command.executed_price,
            )
            response = client.place_order(order_request)
        except DhanClientConfigError as exc:
            request_payload = None
            try:
                request_payload = build_dhan_order_request(
                    reviewed_trade=reviewed_trade,
                    signal=signal,
                    fallback_price=command.executed_price,
                ).to_payload()
            except Exception:
                request_payload = {"broker": broker}

            return ExecutionAdapterResult(
                broker=broker,
                status="BLOCKED",
                executed_price=command.executed_price,
                executed_at=datetime.now(UTC),
                notes=str(exc),
                audit_request_payload=request_payload,
                audit_response_payload={"error": str(exc)},
            )
        except (DhanClientRequestError, ValueError) as exc:
            return ExecutionAdapterResult(
                broker=broker,
                status="BLOCKED",
                executed_price=command.executed_price,
                executed_at=datetime.now(UTC),
                notes=str(exc),
                audit_request_payload={"broker": broker},
                audit_response_payload={"error": str(exc)},
            )

        mapped = map_dhan_response(
            response,
            broker=broker,
            fallback_price=command.executed_price,
        )
        mapped.executed_at = datetime.now(UTC)
        return mapped


__all__ = ["LiveExecutionAdapter"]
