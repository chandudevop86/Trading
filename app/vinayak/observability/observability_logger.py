from __future__ import annotations

import json
import os
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from vinayak.observability.observability_metrics import increment_metric


def _now_iso() -> str:
    return datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def _log_path() -> Path:
    raw = str(os.getenv('VINAYAK_OBSERVABILITY_DIR', '') or '').strip()
    if raw:
        path = Path(raw)
    else:
        path = Path(__file__).resolve().parents[1] / 'data' / 'observability'
    path.mkdir(parents=True, exist_ok=True)
    return path / 'events.jsonl'


def log_event(
    *,
    component: str,
    event_name: str,
    symbol: str = '',
    strategy: str = '',
    severity: str = 'INFO',
    message: str = '',
    context_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = {
        'timestamp': _now_iso(),
        'component': str(component or ''),
        'event_name': str(event_name or ''),
        'symbol': str(symbol or ''),
        'strategy': str(strategy or ''),
        'severity': str(severity or 'INFO').upper(),
        'message': str(message or ''),
        'context_json': dict(context_json or {}),
    }
    try:
        with _log_path().open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(event, default=str) + '\n')
        if event['severity'] in {'ERROR', 'CRITICAL'}:
            increment_metric('trading_cycle_failures_total', 1)
        if event['severity'] in {'WARNING', 'ERROR', 'CRITICAL'}:
            increment_metric('alerts_recent_total', 1)
    except Exception:
        pass
    return event


def log_exception(
    *,
    component: str,
    event_name: str,
    exc: BaseException,
    symbol: str = '',
    strategy: str = '',
    message: str = '',
    context_json: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(context_json or {})
    payload['exception_type'] = type(exc).__name__
    payload['traceback'] = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    increment_metric('exceptions_total', 1)
    return log_event(
        component=component,
        event_name=event_name,
        symbol=symbol,
        strategy=strategy,
        severity='ERROR',
        message=message or str(exc),
        context_json=payload,
    )


def tail_events(limit: int = 25, *, severities: set[str] | None = None) -> list[dict[str, Any]]:
    path = _log_path()
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open('r', encoding='utf-8') as handle:
            for raw in handle:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except Exception:
                    continue
                if severities and str(event.get('severity', '')).upper() not in {s.upper() for s in severities}:
                    continue
                rows.append(event)
    except Exception:
        return []
    return rows[-max(int(limit), 1):]


__all__ = ['log_event', 'log_exception', 'tail_events']
