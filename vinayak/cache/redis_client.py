from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import json
import os

try:
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore


@dataclass(slots=True)
class RedisSettings:
    url: str

    @classmethod
    def from_env(cls) -> 'RedisSettings':
        return cls(url=str(os.getenv('REDIS_URL', '') or '').strip())


class RedisCache:
    def __init__(self, url: str) -> None:
        self.url = str(url or '').strip()
        self._client: Any | None = None

    @classmethod
    def from_env(cls) -> 'RedisCache':
        return cls(RedisSettings.from_env().url)

    def is_configured(self) -> bool:
        return bool(self.url and redis is not None)

    def _get_client(self):
        if not self.is_configured():
            return None
        if self._client is None:
            self._client = redis.Redis.from_url(self.url, decode_responses=True)
        return self._client

    def get_json(self, key: str) -> Any | None:
        client = self._get_client()
        if client is None:
            return None
        try:
            raw = client.get(key)
        except Exception:
            return None
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    def set_json(self, key: str, value: Any, ttl_seconds: int | None = None) -> bool:
        client = self._get_client()
        if client is None:
            return False
        try:
            payload = json.dumps(value)
            if ttl_seconds and int(ttl_seconds) > 0:
                client.setex(key, int(ttl_seconds), payload)
            else:
                client.set(key, payload)
            return True
        except Exception:
            return False


def build_cache_key(*parts: object) -> str:
    cleaned = [str(part or '').strip().replace(' ', '_') for part in parts]
    return ':'.join(part for part in cleaned if part)
