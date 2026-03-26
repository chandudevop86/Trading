# Vinayak Environment Matrix

## Environment Summary

| Environment | Purpose | API Runtime | Database | Cache | Messaging | Public Entry | Workers | MongoDB |
|---|---|---|---|---|---|---|---|---|
| DEV | local feature development | FastAPI + Uvicorn | SQLite or local PostgreSQL | Optional Redis | Disabled or local RabbitMQ | none or localhost | optional | not required |
| UAT | integration testing and validation | FastAPI + Uvicorn | PostgreSQL | Redis | RabbitMQ | Nginx or internal ALB | required | optional |
| PROD | real production traffic | FastAPI + Uvicorn | PostgreSQL | Redis | RabbitMQ | Nginx or AWS ALB | required | optional |

## Required By Environment

### DEV
- keep developer setup simple
- SQLite is acceptable
- Redis is useful but optional
- RabbitMQ can stay disabled if you are only testing API flows
- outbox remains enabled in code but dispatch can be skipped when messaging is disabled

Recommended env file:
- `vinayak/deploy/env/dev.env.example`

### UAT
- use PostgreSQL
- use Redis
- use RabbitMQ
- run both workers
- put Nginx or internal ALB in front
- validate reviewed trades, executions, outbox relay, and queue consumers

Recommended env file:
- `vinayak/deploy/env/uat.env.example`

### PROD
- use PostgreSQL as the primary relational database
- use Redis for cache
- use RabbitMQ for async messaging
- run API, outbox worker, and queue worker separately
- use Nginx or AWS ALB as the only public entry
- keep API private behind the web tier
- keep DB, cache, and broker private
- enable S3-backed report storage when available

Recommended env file:
- `vinayak/deploy/env/prod.env.example`

## Promotion Path

1. Develop locally with DEV settings.
2. Promote to UAT with PostgreSQL, Redis, RabbitMQ, and both workers enabled.
3. Promote to PROD only after health, admin, outbox, and worker flows are validated in UAT.

## Runtime Order

1. Start PostgreSQL.
2. Start Redis.
3. Start RabbitMQ.
4. Run Alembic migrations.
5. Start Vinayak API.
6. Start outbox worker.
7. Start queue worker.
8. Put Nginx or ALB in front.

## Final Recommendation

- DEV: keep minimal
- UAT: mirror production behavior as much as possible
- PROD: keep only PostgreSQL, Redis, RabbitMQ, API, outbox worker, queue worker, and Nginx/ALB
- MongoDB should remain optional until a real document-data requirement appears
