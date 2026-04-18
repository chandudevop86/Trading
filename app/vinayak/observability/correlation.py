from __future__ import annotations

from contextvars import ContextVar
from uuid import uuid4


_CORRELATION_ID: ContextVar[str] = ContextVar('vinayak_correlation_id', default='')


def set_correlation_id(value: str) -> str:
    _CORRELATION_ID.set(value)
    return value


def clear_correlation_id() -> None:
    _CORRELATION_ID.set('')


def get_correlation_id() -> str:
    current = _CORRELATION_ID.get()
    if current:
        return current
    generated = uuid4().hex
    _CORRELATION_ID.set(generated)
    return generated
