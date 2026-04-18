from __future__ import annotations

import time
from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from vinayak.core.config import get_settings
from vinayak.observability.correlation import clear_correlation_id, set_correlation_id
from vinayak.observability.observability_logger import log_event
from vinayak.observability.observability_metrics import increment_metric, set_metric


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Assigns correlation ids and emits structured request logs."""

    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        request_id_header = settings.observability.request_id_header
        request_id = str(request.headers.get(request_id_header) or uuid4().hex).strip() or uuid4().hex
        set_correlation_id(request_id)
        request.state.request_id = request_id
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = round((time.perf_counter() - started) * 1000.0, 2)
            increment_metric('http_request_total', 1, labels={'method': request.method, 'path': request.url.path, 'status': 500})
            increment_metric('http_request_error_total', 1, labels={'method': request.method, 'path': request.url.path, 'status': 500})
            set_metric('http_request_latency_ms', duration_ms, labels={'method': request.method, 'path': request.url.path, 'status': 500})
            log_event(
                component='api',
                event_name='http_request_failed',
                severity='ERROR',
                message='HTTP request failed',
                context_json={
                    'method': request.method,
                    'path': request.url.path,
                    'query': request.url.query,
                    'client': request.client.host if request.client else '',
                    'status_code': 500,
                    'duration_ms': duration_ms,
                    'exception_type': type(exc).__name__,
                },
            )
            clear_correlation_id()
            raise

        duration_ms = round((time.perf_counter() - started) * 1000.0, 2)
        response.headers[request_id_header] = request_id
        severity = 'ERROR' if response.status_code >= 500 else 'WARNING' if response.status_code >= 400 else 'INFO'
        increment_metric('http_request_total', 1, labels={'method': request.method, 'path': request.url.path, 'status': response.status_code})
        if response.status_code >= 400:
            increment_metric('http_request_error_total', 1, labels={'method': request.method, 'path': request.url.path, 'status': response.status_code})
        set_metric('http_request_latency_ms', duration_ms, labels={'method': request.method, 'path': request.url.path, 'status': response.status_code})
        log_event(
            component='api',
            event_name='http_request',
            severity=severity,
            message='HTTP request completed',
            context_json={
                'method': request.method,
                'path': request.url.path,
                'query': request.url.query,
                'client': request.client.host if request.client else '',
                'status_code': response.status_code,
                'duration_ms': duration_ms,
            },
        )
        clear_correlation_id()
        return response


__all__ = ['RequestContextMiddleware']
