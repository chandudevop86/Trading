# Environment Variables

## Runtime
- `APP_NAME`: Human-readable application name.
- `APP_ENV`: Runtime environment. Use `development`, `test`, or `production`.
- `APP_HOST`: Bind host for the FastAPI runtime.
- `APP_PORT`: Bind port for the FastAPI runtime.

## Authentication
- `VINAYAK_ADMIN_USERNAME`: Default admin username used to seed the first admin account.
- `VINAYAK_ADMIN_PASSWORD`: Default admin password. Must not be a placeholder value.
- `VINAYAK_ADMIN_SECRET`: Secret used to sign session cookies. Must be unique and non-placeholder.
- `VINAYAK_SECURE_COOKIES`: Set `true` in production so session cookies are marked secure.
- `VINAYAK_AUTO_LOGIN`: Development-only shortcut for local auto-login. Ignored and rejected in production.
- `VINAYAK_SYNC_ADMIN_FROM_ENV`: Allows syncing the default admin credentials from env in non-production only.
- `VINAYAK_SESSION_COOKIE_NAME`: Primary session cookie name.
- `VINAYAK_LEGACY_SESSION_COOKIE_NAME`: Legacy admin cookie name still accepted for backwards compatibility.

## Database and persistence
- `VINAYAK_DATABASE_URL`: SQL database URL. Production must not use sqlite.
- `MONGODB_URL`: MongoDB connection URL for catalog data.
- `MONGODB_DATABASE`: Mongo database name.
- `MONGODB_PRODUCT_COLLECTION`: Product collection name.
- `REDIS_URL`: Redis connection URL.
- `REDIS_DEFAULT_TTL_SECONDS`: Default Redis TTL used by cache-backed services.

## Message bus
- `MESSAGE_BUS_ENABLED`: Enables asynchronous event publication.
- `MESSAGE_BUS_BACKEND`: Bus backend identifier such as `rabbitmq`.
- `MESSAGE_BUS_URL`: Connection URL for the message bus.
- `MESSAGE_BUS_TOPIC_PREFIX`: Topic prefix used for emitted events.

## Execution controls
- `VINAYAK_ENABLE_SYNC_LIVE_ANALYSIS`: Development convenience for synchronous live analysis. Must stay disabled in production.
- `VINAYAK_ENABLE_LIVE_TRADING`: Gate for live execution surfaces.
- `VINAYAK_ENABLE_PAPER_TRADING`: Gate for paper execution surfaces.

## Observability
- `VINAYAK_LOG_LEVEL`: Base application log level.
- `VINAYAK_JSON_LOGS`: Enables JSON structured logs.
- `VINAYAK_REQUEST_ID_HEADER`: Header name used for request correlation.
- `VINAYAK_HEALTH_READY_CACHE_TTL_SECONDS`: Readiness cache TTL to avoid probe-driven load spikes.

## Integrations
- `DHAN_CLIENT_ID`: Dhan API client id.
- `DHAN_ACCESS_TOKEN`: Dhan API access token.
- `DHAN_BASE_URL`: Dhan API base URL.
- `DHAN_TIMEOUT`: Timeout for Dhan network calls in seconds.
- `TELEGRAM_TOKEN`: Telegram bot token.
- `TELEGRAM_CHAT_ID`: Telegram target chat id.

## Production fail-fast rules
Vinayak will now refuse production startup when any of the following are true:
- admin username, password, or secret is missing
- admin secret is a placeholder value
- `VINAYAK_AUTO_LOGIN=true`
- `VINAYAK_SYNC_ADMIN_FROM_ENV=true`
- `VINAYAK_SECURE_COOKIES=false`
- `VINAYAK_DATABASE_URL` points at sqlite
- `VINAYAK_ENABLE_SYNC_LIVE_ANALYSIS=true`
- message bus is enabled but `MESSAGE_BUS_URL` is empty
