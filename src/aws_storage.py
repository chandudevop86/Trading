from __future__ import annotations

import mimetypes
import os
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_REGION = 'ap-south-1'
DEFAULT_PREFIX = 'trading'


def build_s3_key(prefix: str, filename: str, now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    safe_prefix = prefix.strip().strip('/')
    ts = now.strftime('%Y%m%d_%H%M%S')
    base = f'{ts}_{filename}'
    return f'{safe_prefix}/{base}' if safe_prefix else base


def _get_s3_client(region: str = DEFAULT_REGION):
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError('boto3 is not installed. Run: py -3 -m pip install boto3') from exc
    session = boto3.session.Session(region_name=region)
    return session.client('s3')


def _safe_bucket(bucket: str) -> str:
    value = bucket.strip()
    if not value:
        raise ValueError('S3 bucket is required')
    return value


def _safe_key(key: str) -> str:
    value = key.strip().lstrip('/')
    if not value:
        raise ValueError('S3 key is required')
    return value


def upload_text_to_s3(
    bucket: str,
    key: str,
    body: str,
    region: str = DEFAULT_REGION,
    *,
    content_type: str = 'text/plain',
) -> str:
    client = _get_s3_client(region)
    bucket_name = _safe_bucket(bucket)
    object_key = _safe_key(key)
    client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=body.encode('utf-8'),
        ContentType=content_type,
    )
    return f's3://{bucket_name}/{object_key}'


def upload_file_to_s3(
    bucket: str,
    key: str,
    source_path: str | Path,
    region: str = DEFAULT_REGION,
    *,
    content_type: str | None = None,
    extra_args: dict[str, Any] | None = None,
) -> str:
    source = Path(source_path)
    if not source.exists():
        raise FileNotFoundError(f'Cannot upload missing file to S3: {source}')
    client = _get_s3_client(region)
    bucket_name = _safe_bucket(bucket)
    object_key = _safe_key(key)
    args = dict(extra_args or {})
    guessed = content_type or mimetypes.guess_type(str(source))[0]
    if guessed:
        args.setdefault('ExtraArgs', {'ContentType': guessed})
    with source.open('rb') as handle:
        client.upload_fileobj(handle, bucket_name, object_key, **args)
    return f's3://{bucket_name}/{object_key}'


def download_file_from_s3(
    bucket: str,
    key: str,
    destination_path: str | Path,
    region: str = DEFAULT_REGION,
) -> Path:
    client = _get_s3_client(region)
    destination = Path(destination_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix='trading-s3-', suffix='.tmp', dir=str(destination.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        with tmp_path.open('wb') as handle:
            client.download_fileobj(_safe_bucket(bucket), _safe_key(key), handle)
        shutil.move(str(tmp_path), str(destination))
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    return destination


def s3_enabled(bucket: str | None = None) -> bool:
    target = (bucket or os.getenv('AWS_S3_BUCKET', '')).strip()
    return bool(target)


def sync_path_to_s3(
    source_path: str | Path,
    *,
    bucket: str,
    prefix: str = DEFAULT_PREFIX,
    region: str = DEFAULT_REGION,
    key_prefix: str = '',
) -> str:
    source = Path(source_path)
    filename = source.name
    parts = [part.strip('/').strip() for part in [prefix, key_prefix] if part and str(part).strip()]
    object_prefix = '/'.join(part for part in parts if part)
    object_key = f'{object_prefix}/{filename}' if object_prefix else filename
    return upload_file_to_s3(bucket, object_key, source, region)


def sync_path_to_s3_if_enabled(
    source_path: str | Path,
    *,
    key_prefix: str = '',
    bucket: str | None = None,
    prefix: str | None = None,
    region: str | None = None,
) -> str | None:
    bucket_name = (bucket or os.getenv('AWS_S3_BUCKET', '')).strip()
    if not bucket_name:
        return None
    object_prefix = prefix or os.getenv('AWS_S3_PREFIX', DEFAULT_PREFIX)
    target_region = region or os.getenv('AWS_REGION', DEFAULT_REGION)
    return sync_path_to_s3(source_path, bucket=bucket_name, prefix=object_prefix, region=target_region, key_prefix=key_prefix)
