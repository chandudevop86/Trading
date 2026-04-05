from __future__ import annotations

import os

from vinayak.messaging.bus import EventEnvelope, build_message_bus
from vinayak.messaging.events import EVENT_NOTIFICATION_REQUESTED
from vinayak.notifications.telegram.notifier import send_text_notification
from vinayak.observability.observability_logger import log_exception


def _env_telegram_target() -> tuple[str, str]:
    token = str(os.getenv('TELEGRAM_BOT_TOKEN', '') or '').strip()
    chat_id = str(os.getenv('TELEGRAM_CHAT_ID', '') or '').strip()
    return token, chat_id


def _handle_event(event: EventEnvelope) -> None:
    if event.name != EVENT_NOTIFICATION_REQUESTED:
        return
    payload = event.payload
    token, chat_id = _env_telegram_target()
    message = str(payload.get('message', '') or '')
    if token and chat_id and message:
        try:
            send_text_notification(token=token, chat_id=chat_id, message=message)
        except Exception as exc:
            log_exception(
                component='event_worker',
                event_name='notification_delivery_failed',
                exc=exc,
                symbol=str(payload.get('symbol', '') or ''),
                strategy=str(payload.get('strategy', '') or ''),
                message='Event worker failed to deliver Telegram notification',
                context_json={'message_preview': message[:200], 'channel': 'telegram'},
            )


def main() -> None:
    bus = build_message_bus()
    bus.consume(_handle_event)


if __name__ == '__main__':
    main()
