from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class SqlSettings:
    url: str
    provider: str


@dataclass(frozen=True)
class MongoSettings:
    url: str
    database: str
    product_collection: str


@dataclass(frozen=True)
class RedisSettings:
    url: str
    default_ttl_seconds: int


@dataclass(frozen=True)
class MessageBusSettings:
    backend: str
    url: str
    topic_prefix: str
    enabled: bool


@dataclass(frozen=True)
class AppSettings:
    env: str
    sql: SqlSettings
    mongo: MongoSettings
    redis: RedisSettings
    message_bus: MessageBusSettings


def _default_sqlite_url() -> str:
    default_path = Path(__file__).resolve().parents[1] / 'data' / 'vinayak.db'
    return f"sqlite:///{default_path.as_posix()}"


def _detect_sql_provider(url: str) -> str:
    lower = str(url or '').lower()
    if lower.startswith('postgresql'):
        return 'postgresql'
    if lower.startswith('mysql'):
        return 'mysql'
    if 'sqlserver' in lower or lower.startswith('mssql'):
        return 'mssql'
    if lower.startswith('sqlite'):
        return 'sqlite'
    return 'unknown'


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    sql_url = str(os.getenv('VINAYAK_DATABASE_URL', _default_sqlite_url()) or _default_sqlite_url()).strip()
    bus_backend = str(os.getenv('MESSAGE_BUS_BACKEND', 'rabbitmq') or 'rabbitmq').strip().lower()
    bus_url = str(
        os.getenv('MESSAGE_BUS_URL')
        or os.getenv('RABBITMQ_URL')
        or os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        or os.getenv('ACTIVEMQ_URL')
        or ''
    ).strip()
    return AppSettings(
        env=str(os.getenv('APP_ENV', 'dev') or 'dev').strip(),
        sql=SqlSettings(url=sql_url, provider=_detect_sql_provider(sql_url)),
        mongo=MongoSettings(
            url=str(os.getenv('MONGODB_URL', 'mongodb://localhost:27017') or '').strip(),
            database=str(os.getenv('MONGODB_DATABASE', 'vinayak') or 'vinayak').strip(),
            product_collection=str(os.getenv('MONGODB_PRODUCT_COLLECTION', 'product_catalog') or 'product_catalog').strip(),
        ),
        redis=RedisSettings(
            url=str(os.getenv('REDIS_URL', '') or '').strip(),
            default_ttl_seconds=_int_env('REDIS_DEFAULT_TTL_SECONDS', 900),
        ),
        message_bus=MessageBusSettings(
            backend=bus_backend,
            url=bus_url,
            topic_prefix=str(os.getenv('MESSAGE_BUS_TOPIC_PREFIX', 'vinayak') or 'vinayak').strip(),
            enabled=str(os.getenv('MESSAGE_BUS_ENABLED', 'true') or 'true').strip().lower() not in {'0', 'false', 'no'},
        ),
    )


def reset_settings_cache() -> None:
    get_settings.cache_clear()
