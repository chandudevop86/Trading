from __future__ import annotations

import time
from typing import Any

import requests

from vinayak.observability.observability_logger import log_event, log_exception

_DEFAULT_TIMEOUT_SECONDS = 10.0
_MAX_ATTEMPTS = 3
_RETRYABLE_EXCEPTIONS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.SSLError,
)


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


def send_text_notification(*, token: str, chat_id: str, message: str, timeout: float = _DEFAULT_TIMEOUT_SECONDS) -> dict[str, object]:
    last_error: BaseException | None = None
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    payload = {'chat_id': chat_id, 'text': message}

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = requests.post(url, json=payload, timeout=float(timeout))
            response.raise_for_status()
            parsed = response.json()
            result = parsed if isinstance(parsed, dict) else {'ok': True}
            if attempt > 1:
                log_event(
                    component='telegram_notifier',
                    event_name='telegram_retry_recovered',
                    severity='WARNING',
                    message='Telegram delivery recovered after retry',
                    context_json={'attempt': attempt, 'chat_id': chat_id, 'payload_size': len(message)},
                )
            return result
        except _RETRYABLE_EXCEPTIONS as exc:
            last_error = exc
            if attempt >= _MAX_ATTEMPTS:
                break
            log_event(
                component='telegram_notifier',
                event_name='telegram_send_retry',
                severity='WARNING',
                message='Retrying Telegram notification after transient transport failure',
                context_json={
                    'attempt': attempt,
                    'max_attempts': _MAX_ATTEMPTS,
                    'exception_type': type(exc).__name__,
                    'chat_id': chat_id,
                    'payload_size': len(message),
                },
            )
            time.sleep(0.25 * attempt)
        except Exception as exc:
            log_exception(
                component='telegram_notifier',
                event_name='telegram_send_nonretryable_failed',
                exc=exc,
                message='Telegram notification failed without retry',
                context_json={'chat_id': chat_id, 'payload_size': len(message)},
            )
            raise

    assert last_error is not None
    log_exception(
        component='telegram_notifier',
        event_name='telegram_send_failed',
        exc=last_error,
        message='Telegram notification failed after retries',
        context_json={'chat_id': chat_id, 'payload_size': len(message), 'max_attempts': _MAX_ATTEMPTS},
    )
    raise last_error
