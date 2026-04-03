from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PIPELINE_STAGES = [
    'market_fetch',
    'dataframe_normalize',
    'indicator_calc',
    'zone_detection',
    'validation',
    'trade_build',
    'execute',
    'notify',
]


def _now_iso() -> str:
    return datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def _base_dir() -> Path:
    raw = str(os.getenv('VINAYAK_OBSERVABILITY_DIR', '') or '').strip()
    if raw:
        path = Path(raw)
    else:
        path = Path(__file__).resolve().parents[1] / 'data' / 'observability'
    path.mkdir(parents=True, exist_ok=True)
    return path


def _snapshot_path() -> Path:
    return _base_dir() / 'metrics_snapshot.json'


def _default_snapshot() -> dict[str, Any]:
    return {
        'updated_at': _now_iso(),
        'metrics': {},
        'stages': {
            stage: {
                'status': 'UNKNOWN',
                'duration_seconds': 0.0,
                'last_success': '',
                'last_failure': '',
                'message': '',
                'symbol': '',
                'strategy': '',
                'trace_id': '',
            }
            for stage in PIPELINE_STAGES
        },
        'recent_stage_events': [],
    }


def _read_snapshot() -> dict[str, Any]:
    path = _snapshot_path()
    if not path.exists():
        data = _default_snapshot()
        path.write_text(json.dumps(data, indent=2), encoding='utf-8')
        return data
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(data, dict):
            raise ValueError('snapshot must be dict')
        data.setdefault('metrics', {})
        data.setdefault('stages', {})
        data.setdefault('recent_stage_events', [])
        for stage in PIPELINE_STAGES:
            data['stages'].setdefault(stage, {
                'status': 'UNKNOWN',
                'duration_seconds': 0.0,
                'last_success': '',
                'last_failure': '',
                'message': '',
                'symbol': '',
                'strategy': '',
                'trace_id': '',
            })
        return data
    except Exception:
        data = _default_snapshot()
        path.write_text(json.dumps(data, indent=2), encoding='utf-8')
        return data


def _write_snapshot(snapshot: dict[str, Any]) -> None:
    snapshot['updated_at'] = _now_iso()
    _snapshot_path().write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding='utf-8')


def reset_observability_state() -> None:
    path = _base_dir()
    for name in ('metrics_snapshot.json', 'events.jsonl'):
        target = path / name
        if target.exists():
            target.unlink()
    _write_snapshot(_default_snapshot())


def set_metric(name: str, value: float | int | str | bool, *, kind: str = 'gauge', labels: dict[str, Any] | None = None) -> None:
    snapshot = _read_snapshot()
    snapshot['metrics'][str(name)] = {
        'value': value,
        'kind': kind,
        'labels': dict(labels or {}),
        'updated_at': _now_iso(),
    }
    _write_snapshot(snapshot)


def increment_metric(name: str, value: float = 1.0, *, labels: dict[str, Any] | None = None) -> None:
    snapshot = _read_snapshot()
    current = snapshot['metrics'].get(str(name), {}).get('value', 0.0)
    try:
        numeric = float(current)
    except Exception:
        numeric = 0.0
    snapshot['metrics'][str(name)] = {
        'value': numeric + float(value),
        'kind': 'counter',
        'labels': dict(labels or {}),
        'updated_at': _now_iso(),
    }
    _write_snapshot(snapshot)


def get_metric(name: str, default: Any = 0) -> Any:
    snapshot = _read_snapshot()
    return snapshot.get('metrics', {}).get(str(name), {}).get('value', default)


def record_stage(
    stage: str,
    *,
    status: str,
    duration_seconds: float = 0.0,
    symbol: str = '',
    strategy: str = '',
    message: str = '',
    trace_id: str = '',
) -> None:
    stage_name = str(stage).strip() or 'unknown_stage'
    snapshot = _read_snapshot()
    stage_state = snapshot['stages'].setdefault(stage_name, {
        'status': 'UNKNOWN',
        'duration_seconds': 0.0,
        'last_success': '',
        'last_failure': '',
        'message': '',
        'symbol': '',
        'strategy': '',
        'trace_id': '',
    })
    now = _now_iso()
    stage_state.update({
        'status': str(status).upper(),
        'duration_seconds': round(float(duration_seconds or 0.0), 4),
        'message': str(message or ''),
        'symbol': str(symbol or ''),
        'strategy': str(strategy or ''),
        'trace_id': str(trace_id or ''),
    })
    if str(status).upper() == 'SUCCESS':
        stage_state['last_success'] = now
    if str(status).upper() in {'FAIL', 'FAILED', 'ERROR'}:
        stage_state['last_failure'] = now
    snapshot['recent_stage_events'] = (snapshot.get('recent_stage_events', []) + [{
        'timestamp': now,
        'stage': stage_name,
        'status': str(status).upper(),
        'duration_seconds': round(float(duration_seconds or 0.0), 4),
        'symbol': str(symbol or ''),
        'strategy': str(strategy or ''),
        'message': str(message or ''),
        'trace_id': str(trace_id or ''),
    }])[-50:]
    _write_snapshot(snapshot)


def get_observability_snapshot() -> dict[str, Any]:
    return _read_snapshot()


__all__ = [
    'PIPELINE_STAGES',
    'get_metric',
    'get_observability_snapshot',
    'increment_metric',
    'record_stage',
    'reset_observability_state',
    'set_metric',
]
