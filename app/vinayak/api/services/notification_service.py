from __future__ import annotations

"""Notification services for live analysis workflows."""

from typing import Any, Callable

from vinayak.messaging.events import EVENT_NOTIFICATION_REQUESTED


def dispatch_signal_summary(
    *,
    send_telegram: bool,
    signal_rows: list[dict[str, Any]],
    symbol: str,
    strategy: str,
    telegram_token: str,
    telegram_chat_id: str,
    deliver_telegram_inline: bool,
    build_trade_summary_fn: Callable[[list[dict[str, Any]]], str],
    send_telegram_message_fn: Callable[[str, str, str], dict[str, Any]],
    log_exception_fn: Callable[..., None],
    message_bus: Any,
) -> dict[str, Any]:
    telegram_sent = False
    telegram_error = ""
    telegram_payload: dict[str, Any] | None = None
    summary_text = build_trade_summary_fn(signal_rows) if signal_rows else "No signals generated for this run."
    if send_telegram and signal_rows:
        try:
            message_bus.publish(
                EVENT_NOTIFICATION_REQUESTED,
                {
                    "channel": "telegram",
                    "message": summary_text,
                    "symbol": symbol,
                    "strategy": strategy,
                },
                source="live_analysis",
            )
            if deliver_telegram_inline:
                try:
                    telegram_payload = send_telegram_message_fn(telegram_token, telegram_chat_id, summary_text)
                    telegram_sent = True
                except Exception as exc:
                    telegram_error = str(exc)
                    log_exception_fn(
                        component="trading_workspace",
                        event_name="telegram_send_failed",
                        exc=exc,
                        symbol=symbol,
                        strategy=strategy,
                        message="Telegram delivery failed during live analysis",
                        context_json={"message_preview": summary_text[:200], "signal_count": len(signal_rows)},
                    )
            else:
                telegram_payload = {"queued": True, "channel": "telegram"}
        except Exception as exc:
            telegram_error = str(exc)
            log_exception_fn(
                component="trading_workspace",
                event_name="telegram_summary_failed",
                exc=exc,
                symbol=symbol,
                strategy=strategy,
                message="Telegram summary generation failed during live analysis",
                context_json={"signal_count": len(signal_rows)},
            )
    return {
        "summary_text": summary_text,
        "telegram_sent": telegram_sent,
        "telegram_error": telegram_error,
        "telegram_payload": telegram_payload or {},
    }

