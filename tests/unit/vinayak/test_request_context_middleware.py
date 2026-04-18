from __future__ import annotations

from fastapi.testclient import TestClient

from vinayak.api.main import app
from vinayak.api.middleware import request_context as request_context_module
from vinayak.observability.observability_metrics import get_metric, reset_observability_state


def test_request_context_middleware_sets_request_id_header(monkeypatch, tmp_path) -> None:
    captured: list[dict[str, object]] = []
    monkeypatch.setenv('VINAYAK_OBSERVABILITY_DIR', str(tmp_path / 'observability'))
    reset_observability_state()

    def _capture(**kwargs):
        captured.append(kwargs)
        return kwargs

    monkeypatch.setattr(request_context_module, 'log_event', _capture)

    with TestClient(app) as client:
        response = client.get('/health/live', headers={'X-Request-ID': 'req-123'})

    assert response.status_code == 200
    assert response.headers['X-Request-ID'] == 'req-123'
    assert captured
    assert captured[-1]['event_name'] == 'http_request'
    assert captured[-1]['context_json']['path'] == '/health/live'
    assert captured[-1]['context_json']['status_code'] == 200
    assert float(get_metric('http_request_total', 0)) >= 1.0
    assert float(get_metric('http_request_latency_ms', 0)) >= 0.0


def test_request_context_middleware_records_error_metric_for_404(monkeypatch, tmp_path) -> None:
    captured: list[dict[str, object]] = []
    monkeypatch.setenv('VINAYAK_OBSERVABILITY_DIR', str(tmp_path / 'observability'))
    reset_observability_state()

    def _capture(**kwargs):
        captured.append(kwargs)
        return kwargs

    monkeypatch.setattr(request_context_module, 'log_event', _capture)

    with TestClient(app) as client:
        response = client.get('/does-not-exist')

    assert response.status_code == 404
    assert captured[-1]['event_name'] == 'http_request'
    assert captured[-1]['context_json']['status_code'] == 404
    assert float(get_metric('http_request_total', 0)) >= 1.0
    assert float(get_metric('http_request_error_total', 0)) >= 1.0
