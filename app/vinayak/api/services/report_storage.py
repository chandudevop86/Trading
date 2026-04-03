from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import json
import os

from vinayak.cache.redis_client import RedisCache


def _safe_name(text: str) -> str:
    return ''.join(ch if ch.isalnum() or ch in {'-', '_'} else '_' for ch in str(text or '').strip()) or 'artifact'


def _reports_dir() -> Path:
    return Path(os.getenv('REPORTS_DIR', 'app/vinayak/data/reports'))


def _bucket() -> str:
    return str(os.getenv('REPORTS_S3_BUCKET', '') or '').strip()


def _region() -> str:
    return str(os.getenv('AWS_REGION', 'ap-south-1') or 'ap-south-1').strip()


def _prefix() -> str:
    return str(os.getenv('REPORTS_S3_PREFIX', 'vinayak/reports') or 'vinayak/reports').strip().strip('/')


def _s3_key(filename: str, now: datetime | None = None) -> str:
    stamp = (now or datetime.now(UTC)).strftime('%Y/%m/%d/%H%M%S')
    prefix = _prefix()
    return f'{prefix}/{stamp}_{filename}' if prefix else f'{stamp}_{filename}'


def _upload_bytes_to_s3(bucket: str, key: str, body: bytes, content_type: str) -> str:
    import boto3
    session = boto3.session.Session(region_name=_region())
    client = session.client('s3')
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    return f's3://{bucket}/{key}'


def store_text_report(name: str, body: str, *, extension: str = 'txt', content_type: str = 'text/plain') -> dict[str, str]:
    now = datetime.now(UTC)
    safe_name = _safe_name(name)
    filename = f"{now.strftime('%Y%m%d_%H%M%S')}_{safe_name}.{extension.strip('.') or 'txt'}"
    local_dir = _reports_dir()
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / filename
    local_path.write_text(body, encoding='utf-8')

    result: dict[str, str] = {'local_path': str(local_path)}
    bucket = _bucket()
    if bucket:
        try:
            result['s3_uri'] = _upload_bytes_to_s3(bucket, _s3_key(filename, now=now), body.encode('utf-8'), content_type)
        except Exception as exc:
            result['s3_error'] = str(exc)
    return result


def store_json_report(name: str, payload: Any) -> dict[str, str]:
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    return store_text_report(name, text, extension='json', content_type='application/json')


def cache_json_artifact(name: str, payload: Any, *, ttl_seconds: int = 900) -> bool:
    cache = RedisCache.from_env()
    if not cache.is_configured():
        return False
    return cache.set_json(f'vinayak:artifact:{_safe_name(name)}', payload, ttl_seconds=ttl_seconds)

