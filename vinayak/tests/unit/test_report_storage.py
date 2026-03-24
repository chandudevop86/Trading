from __future__ import annotations

import json
from pathlib import Path

from vinayak.api.services import report_storage as service


class _StubCache:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, int | None]] = []

    def is_configured(self) -> bool:
        return True

    def set_json(self, key: str, value, ttl_seconds: int | None = None) -> bool:
        self.calls.append((key, value, ttl_seconds))
        return True


def test_store_json_report_writes_local_file_and_s3(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv('REPORTS_DIR', str(tmp_path))
    monkeypatch.setenv('REPORTS_S3_BUCKET', 'vinayak-artifacts')
    monkeypatch.setenv('REPORTS_S3_PREFIX', 'reports')

    uploads: list[tuple[str, str, bytes, str]] = []

    def _upload(bucket: str, key: str, body: bytes, content_type: str) -> str:
        uploads.append((bucket, key, body, content_type))
        return f's3://{bucket}/{key}'

    monkeypatch.setattr(service, '_upload_bytes_to_s3', _upload)

    result = service.store_json_report('latest_analysis', {'signal_count': 2})

    assert result['local_path'].endswith('.json')
    assert 's3_uri' in result
    assert uploads[0][0] == 'vinayak-artifacts'
    assert uploads[0][3] == 'application/json'
    assert json.loads(Path(result['local_path']).read_text(encoding='utf-8'))['signal_count'] == 2


def test_cache_json_artifact_uses_redis_when_available(monkeypatch) -> None:
    cache = _StubCache()
    monkeypatch.setattr(service, 'RedisCache', type('RedisCacheStub', (), {'from_env': staticmethod(lambda: cache)}))

    ok = service.cache_json_artifact('latest_live_analysis', {'status': 'ok'}, ttl_seconds=60)

    assert ok is True
    assert cache.calls[0][0] == 'vinayak:artifact:latest_live_analysis'
    assert cache.calls[0][2] == 60
