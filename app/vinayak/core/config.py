from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional import safety during partial environments
    load_dotenv = None


INSECURE_SECRET_VALUES = {
    '',
    'change-me',
    'replace-me',
    'changeme',
    'vinayak-admin-secret',
    'vinayak-dev-secret',
}


class SettingsValidationError(RuntimeError):
    """Raised when runtime configuration is unsafe for startup."""


@dataclass(frozen=True)
class RuntimeSettings:
    environment: str
    app_name: str
    host: str
    port: int

    @property
    def normalized_environment(self) -> str:
        return self.environment.strip().lower()

    @property
    def is_production(self) -> bool:
        return self.normalized_environment in {'prod', 'production'}

    @property
    def is_development_like(self) -> bool:
        return self.normalized_environment in {'local', 'dev', 'development', 'test'}


@dataclass(frozen=True)
class AuthSettings:
    admin_username: str
    admin_password: str
    admin_secret: str
    auto_login_enabled: bool
    sync_admin_from_env: bool
    secure_cookies: bool
    session_cookie_name: str
    legacy_session_cookie_name: str


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
class ExecutionSettings:
    enable_sync_live_analysis: bool
    live_trading_enabled: bool
    paper_trading_enabled: bool


@dataclass(frozen=True)
class ObservabilitySettings:
    log_level: str
    json_logs: bool
    request_id_header: str
    readiness_cache_ttl_seconds: int


@dataclass(frozen=True)
class IntegrationSettings:
    dhan_client_id: str
    dhan_access_token: str
    dhan_base_url: str
    dhan_timeout_seconds: int
    telegram_token: str
    telegram_chat_id: str


@dataclass(frozen=True)
class AppSettings:
    runtime: RuntimeSettings
    auth: AuthSettings
    sql: SqlSettings
    mongo: MongoSettings
    redis: RedisSettings
    message_bus: MessageBusSettings
    execution: ExecutionSettings
    observability: ObservabilitySettings
    integrations: IntegrationSettings

    @property
    def env(self) -> str:
        return self.runtime.environment

    @property
    def legacy_sync_live_analysis_enabled(self) -> bool:
        return self.execution.enable_sync_live_analysis


def _load_environment_files() -> None:
    if load_dotenv is None:
        return

    base_dir = Path(__file__).resolve().parents[1]
    repo_root = base_dir.parents[1]
    candidate_paths = (
        repo_root / '.env',
        base_dir / '.env',
    )
    for path in candidate_paths:
        if path.exists():
            load_dotenv(path, override=False)


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


def _str_env(name: str, default: str = '') -> str:
    return str(os.getenv(name, default) or default).strip()


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() not in {'0', 'false', 'no', 'off'}


def _is_insecure_secret(value: str) -> bool:
    return value.strip().lower() in INSECURE_SECRET_VALUES


def should_auto_initialize_database(*, env: str | None = None, provider: str | None = None) -> bool:
    settings = get_settings()
    resolved_env = str(env or settings.runtime.environment or 'dev').strip().lower()
    resolved_provider = str(provider or settings.sql.provider or '').strip().lower()
    if resolved_env in {'local', 'dev', 'development', 'test'}:
        return True
    return resolved_provider == 'sqlite'


def should_enable_legacy_sync_live_analysis(*, env: str | None = None) -> bool:
    settings = get_settings()
    resolved_env = str(env or settings.runtime.environment or 'dev').strip().lower()
    default_enabled = resolved_env in {'local', 'dev', 'development', 'test'}
    return _bool_env('VINAYAK_ENABLE_SYNC_LIVE_ANALYSIS', default_enabled)


def validate_settings(*, startup: bool = False) -> AppSettings:
    settings = get_settings()
    runtime = settings.runtime
    auth = settings.auth

    errors: list[str] = []
    if runtime.is_production:
        if not auth.admin_username:
            errors.append('VINAYAK_ADMIN_USERNAME must be configured in production.')
        if not auth.admin_password:
            errors.append('VINAYAK_ADMIN_PASSWORD must be configured in production.')
        if not auth.admin_secret:
            errors.append('VINAYAK_ADMIN_SECRET must be configured in production.')
        if _is_insecure_secret(auth.admin_secret):
            errors.append('VINAYAK_ADMIN_SECRET must not use an insecure placeholder in production.')
        if auth.auto_login_enabled:
            errors.append('VINAYAK_AUTO_LOGIN must be disabled in production.')
        if auth.sync_admin_from_env:
            errors.append('VINAYAK_SYNC_ADMIN_FROM_ENV must be disabled in production.')
        if not auth.secure_cookies:
            errors.append('VINAYAK_SECURE_COOKIES must be enabled in production.')
        if settings.sql.provider == 'sqlite':
            errors.append('VINAYAK_DATABASE_URL must not use sqlite in production.')
        if settings.execution.enable_sync_live_analysis:
            errors.append('VINAYAK_ENABLE_SYNC_LIVE_ANALYSIS must be disabled in production.')
        if settings.message_bus.enabled and not settings.message_bus.url:
            errors.append('MESSAGE_BUS_URL must be configured when MESSAGE_BUS_ENABLED is true in production.')

    if startup and errors:
        raise SettingsValidationError(' '.join(errors))
    return settings


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    _load_environment_files()
    sql_url = _str_env('VINAYAK_DATABASE_URL', _default_sqlite_url())
    bus_backend = _str_env('MESSAGE_BUS_BACKEND', 'rabbitmq').lower()
    bus_url = str(
        os.getenv('MESSAGE_BUS_URL')
        or os.getenv('RABBITMQ_URL')
        or os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        or os.getenv('ACTIVEMQ_URL')
        or ''
    ).strip()
    runtime = RuntimeSettings(
        environment=_str_env('APP_ENV', 'dev'),
        app_name=_str_env('APP_NAME', 'Vinayak Trading Platform'),
        host=_str_env('APP_HOST', '0.0.0.0'),
        port=_int_env('APP_PORT', 8000),
    )
    default_sync_live_analysis = runtime.is_development_like
    return AppSettings(
        runtime=runtime,
        auth=AuthSettings(
            admin_username=_str_env('VINAYAK_ADMIN_USERNAME', ''),
            admin_password=_str_env('VINAYAK_ADMIN_PASSWORD', ''),
            admin_secret=_str_env('VINAYAK_ADMIN_SECRET', ''),
            auto_login_enabled=_bool_env('VINAYAK_AUTO_LOGIN', False),
            sync_admin_from_env=_bool_env('VINAYAK_SYNC_ADMIN_FROM_ENV', False),
            secure_cookies=_bool_env('VINAYAK_SECURE_COOKIES', True),
            session_cookie_name=_str_env('VINAYAK_SESSION_COOKIE_NAME', 'vinayak_session'),
            legacy_session_cookie_name=_str_env('VINAYAK_LEGACY_SESSION_COOKIE_NAME', 'vinayak_admin_session'),
        ),
        sql=SqlSettings(url=sql_url, provider=_detect_sql_provider(sql_url)),
        mongo=MongoSettings(
            url=_str_env('MONGODB_URL', 'mongodb://localhost:27017'),
            database=_str_env('MONGODB_DATABASE', 'vinayak'),
            product_collection=_str_env('MONGODB_PRODUCT_COLLECTION', 'product_catalog'),
        ),
        redis=RedisSettings(
            url=_str_env('REDIS_URL', ''),
            default_ttl_seconds=_int_env('REDIS_DEFAULT_TTL_SECONDS', 900),
        ),
        message_bus=MessageBusSettings(
            backend=bus_backend,
            url=bus_url,
            topic_prefix=_str_env('MESSAGE_BUS_TOPIC_PREFIX', 'vinayak'),
            enabled=_bool_env('MESSAGE_BUS_ENABLED', True),
        ),
        execution=ExecutionSettings(
            enable_sync_live_analysis=_bool_env('VINAYAK_ENABLE_SYNC_LIVE_ANALYSIS', default_sync_live_analysis),
            live_trading_enabled=_bool_env('VINAYAK_ENABLE_LIVE_TRADING', False),
            paper_trading_enabled=_bool_env('VINAYAK_ENABLE_PAPER_TRADING', True),
        ),
        observability=ObservabilitySettings(
            log_level=_str_env('VINAYAK_LOG_LEVEL', 'INFO'),
            json_logs=_bool_env('VINAYAK_JSON_LOGS', True),
            request_id_header=_str_env('VINAYAK_REQUEST_ID_HEADER', 'X-Request-ID'),
            readiness_cache_ttl_seconds=_int_env('VINAYAK_HEALTH_READY_CACHE_TTL_SECONDS', 10),
        ),
        integrations=IntegrationSettings(
            dhan_client_id=_str_env('DHAN_CLIENT_ID', ''),
            dhan_access_token=_str_env('DHAN_ACCESS_TOKEN', ''),
            dhan_base_url=_str_env('DHAN_BASE_URL', 'https://api-hq.dhan.co'),
            dhan_timeout_seconds=_int_env('DHAN_TIMEOUT', 30),
            telegram_token=_str_env('TELEGRAM_TOKEN', ''),
            telegram_chat_id=_str_env('TELEGRAM_CHAT_ID', ''),
        ),
    )


def reset_settings_cache() -> None:
    get_settings.cache_clear()
