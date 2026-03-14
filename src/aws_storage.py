from __future__ import annotations

from datetime import UTC, datetime


def build_s3_key(prefix: str, filename: str, now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    safe_prefix = prefix.strip().strip("/")
    ts = now.strftime("%Y%m%d_%H%M%S")
    base = f"{ts}_{filename}"
    return f"{safe_prefix}/{base}" if safe_prefix else base


def upload_text_to_s3(
    bucket: str,
    key: str,
    body: str,
    region: str = "ap-south-1",
) -> str:
    bucket = bucket.strip()
    key = key.strip().lstrip("/")
    if not bucket:
        raise ValueError("S3 bucket is required")
    if not key:
        raise ValueError("S3 key is required")

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
    except ImportError as exc:
        raise RuntimeError("boto3 is not installed. Run: py -3 -m pip install boto3") from exc

    try:
        session = boto3.session.Session(region_name=region)
        client = session.client("s3")
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="text/csv",
        )
        return f"s3://{bucket}/{key}"
    except NoCredentialsError as exc:
        raise RuntimeError("AWS credentials not found. Configure AWS CLI profile or env vars.") from exc
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"S3 upload failed: {exc}") from exc
