from __future__ import annotations

from vinayak.messaging.bus import EventEnvelope, build_message_bus
from vinayak.notifications.telegram.notifier import send_text_notification


def _handle_event(event: EventEnvelope) -> None:
    if event.name != EVENT_NOTIFICATION_REQUESTED:
        return
    payload = event.payload
    token = str(payload.get('telegram_token', '') or '')
    chat_id = str(payload.get('telegram_chat_id', '') or '')
    message = str(payload.get('message', '') or '')
    if token and chat_id and message:
        send_text_notification(token=token, chat_id=chat_id, message=message)


def main() -> None:
    bus = build_message_bus()
    bus.consume(_handle_event)


if __name__ == '__main__':
    main()
