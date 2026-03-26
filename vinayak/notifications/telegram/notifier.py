from __future__ import annotations

import requests


class TelegramNotifier:
    def __init__(self, token: str | None, chat_id: str | None) -> None:
        self.token = token
        self.chat_id = chat_id

    def is_ready(self) -> bool:
        return bool(self.token and self.chat_id)

    def send(self, message: str) -> dict[str, object]:
        if not self.is_ready():
            raise ValueError('Telegram notifier is not configured.')
        return send_text_notification(token=str(self.token), chat_id=str(self.chat_id), message=message)


def send_text_notification(*, token: str, chat_id: str, message: str) -> dict[str, object]:
    response = requests.post(
        f'https://api.telegram.org/bot{token}/sendMessage',
        json={'chat_id': chat_id, 'text': message},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, dict) else {'ok': True}
