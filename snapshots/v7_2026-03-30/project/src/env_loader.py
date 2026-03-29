from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def _strip_wrapped(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def load_env_file(path: str | Path = '.env', *, override: bool = False) -> Path:
    env_path = Path(path)
    if not env_path.is_absolute():
        env_path = Path(__file__).resolve().parent.parent / env_path
    if not env_path.exists():
        return env_path

    for raw_line in env_path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        value = _strip_wrapped(value.strip())
        if not key:
            continue
        if override or key not in os.environ:
            os.environ[key] = value
    return env_path


def _get_boto3_client(service_name: str, region_name: str | None = None):
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError('boto3 is required for AWS secret loading.') from exc
    session = boto3.session.Session(region_name=region_name)
    return session.client(service_name)


def load_ssm_parameters(parameter_path: str, *, region_name: str | None = None, recursive: bool = True, decrypt: bool = True, override: bool = False) -> dict[str, str]:
    path = str(parameter_path or '').strip()
    if not path:
        return {}
    client = _get_boto3_client('ssm', region_name)
    loaded: dict[str, str] = {}
    next_token: str | None = None
    while True:
        kwargs: dict[str, Any] = {
            'Path': path,
            'Recursive': recursive,
            'WithDecryption': decrypt,
            'MaxResults': 10,
        }
        if next_token:
            kwargs['NextToken'] = next_token
        response = client.get_parameters_by_path(**kwargs)
        for item in response.get('Parameters', []):
            full_name = str(item.get('Name', '') or '').strip()
            value = str(item.get('Value', '') or '')
            if not full_name:
                continue
            env_key = full_name.split('/')[-1].strip().upper().replace('-', '_')
            if not env_key:
                continue
            if override or env_key not in os.environ:
                os.environ[env_key] = value
            loaded[env_key] = value
        next_token = response.get('NextToken')
        if not next_token:
            break
    return loaded


def load_secrets_manager_secret(secret_id: str, *, region_name: str | None = None, override: bool = False) -> dict[str, str]:
    secret_name = str(secret_id or '').strip()
    if not secret_name:
        return {}
    client = _get_boto3_client('secretsmanager', region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_text = str(response.get('SecretString', '') or '')
    if not secret_text:
        binary_secret = response.get('SecretBinary')
        if binary_secret is None:
            return {}
        secret_text = binary_secret.decode('utf-8') if hasattr(binary_secret, 'decode') else str(binary_secret)
    try:
        payload = json.loads(secret_text)
    except json.JSONDecodeError:
        payload = {'SECRET_VALUE': secret_text}

    loaded: dict[str, str] = {}
    for key, value in payload.items():
        env_key = str(key).strip().upper().replace('-', '_')
        env_value = str(value)
        if not env_key:
            continue
        if override or env_key not in os.environ:
            os.environ[env_key] = env_value
        loaded[env_key] = env_value
    return loaded


def load_aws_runtime_secrets(*, override: bool = False) -> dict[str, str]:
    loaded: dict[str, str] = {}
    region = os.getenv('AWS_REGION', '').strip() or None
    ssm_path = os.getenv('AWS_SSM_PARAMETER_PATH', '').strip()
    secret_id = os.getenv('AWS_SECRETS_MANAGER_ID', '').strip()

    if ssm_path:
        loaded.update(load_ssm_parameters(ssm_path, region_name=region, override=override))
    if secret_id:
        loaded.update(load_secrets_manager_secret(secret_id, region_name=region, override=override))
    return loaded
