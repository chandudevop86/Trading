# Vinayak Production Architecture

## Service Boundaries

- `web tier`: Nginx or ALB terminates public traffic and forwards requests to the API.
- `api tier`: FastAPI application handles admin UI, strategy APIs, reviewed trades, executions, health, and catalog endpoints.
- `queue worker tier`: background queue worker consumes brokered messages for notifications and async workflows.
- `outbox relay tier`: background outbox worker relays committed domain events from SQL storage to the message bus.
- `relational data tier`: PostgreSQL/MySQL/MSSQL can back transactional entities such as signals, reviewed trades, executions, users, audit logs, and the transactional outbox.
- `document data tier`: MongoDB stores flexible catalog or product-style documents.
- `cache tier`: Redis stores hot OHLCV data, report artifacts, and ephemeral lookup/session-style data.
- `messaging tier`: RabbitMQ is the default runtime broker, with pluggable Kafka/ActiveMQ-aware bus boundaries.

## Current Code Mapping

- SQL persistence: `vinayak/db/`
- Mongo catalog service: `vinayak/catalog/service.py`
- Redis cache utilities: `vinayak/cache/redis_client.py`
- Message bus abstraction: `vinayak/messaging/bus.py`
- Transactional outbox: `vinayak/messaging/outbox.py`
- Queue worker: `vinayak/workers/event_worker.py`
- Outbox relay worker: `vinayak/workers/outbox_worker.py`
- API routes: `vinayak/api/routes/`

## Data Flow

1. Browser traffic reaches Nginx or ALB.
2. Web tier forwards authenticated traffic to FastAPI.
3. Transactional writes go to the relational database.
4. The same SQL transaction writes domain events into `outbox_events`.
5. The outbox worker reads committed pending events and publishes them to the message bus.
6. Queue workers consume published events for downstream async behavior.
7. Flexible catalog writes go to MongoDB.
8. Hot reads and artifact snapshots go to Redis.

## Default Runtime Stack

- Web tier: `Nginx` or `AWS ALB`
- API tier: `FastAPI + Uvicorn`
- Queue worker tier: `python -m vinayak.workers.event_worker`
- Outbox relay tier: `python -m vinayak.workers.outbox_worker`
- RDBMS: `PostgreSQL` by default, with MySQL/MSSQL-compatible SQLAlchemy URLs supported
- Document store: `MongoDB`
- Cache: `Redis`
- Message bus: `RabbitMQ`
