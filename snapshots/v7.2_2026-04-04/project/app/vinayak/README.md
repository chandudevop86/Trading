# Vinayak Trading Platform

`Vinayak` is the next-stage architecture base for the KRSH trading workspace.

This package now lives inside the application tier so we can:

- keep `v3` stable
- build `v4` architecture in parallel
- roll back safely if a future update gets worse

## Architecture

### Web Tier

- Nginx reverse proxy
- Web UI entrypoint
- future React/frontend expansion

### App Tier

- Python API and services
- strategy engine
- execution engine
- broker integrations
- Telegram notifications

### DB Tier

- PostgreSQL for signals, trades, executions, settings
- Redis for cache and fast live state

### Messaging Tier

- RabbitMQ event flow for signals, routing, and alerts

## Initial Layout

```text
app/vinayak/
  web/
  api/
  strategies/
  execution/
  notifications/
  db/
  cache/
  queue/
  data/
  tests/
```

## Recommended Build Order

1. Stabilize API routes and health checks.
2. Move existing strategy logic into `strategies/`.
3. Add execution adapters under `execution/`.
4. Add PostgreSQL models and repositories.
5. Add Redis cache helpers.
6. Add RabbitMQ events and workers.
7. Put Nginx and Docker deployment in front.

## Environment Profiles

Vinayak now has explicit environment examples and Compose entry points:

- DEV env: `infra/app/env/dev.env.example`
- UAT env: `infra/app/env/uat.env.example`
- PROD env: `infra/app/env/prod.env.example`
- DEV compose: `infra/app/docker/docker-compose.dev.yml`
- UAT compose: `infra/app/docker/docker-compose.uat.yml`
- PROD compose: `infra/app/docker/docker-compose.prod.yml`
- Reference matrix: `docs/vinayak_app_reference/environment_matrix.md`

## v3 Baseline

Current stable rollback snapshot:

- `F:\Trading\snapshots\v3_base_2026-03-21\project`
- git tag: `v3-base-2026-03-21`

## Local Run

### Python Run

1. Copy `.env.example` to `.env`
2. Install dependencies:
   - `py -3 -m pip install -r requirements.txt`
3. Start the API:
   - `py -3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000`
4. Open:
   - API root: `http://localhost:8000`
   - Admin console: `http://localhost:8000/admin`

### Docker Compose Run

#### DEV

1. Review `infra/app/env/dev.env.example`
2. Start the stack:
   - `docker compose -f infra/app/docker/docker-compose.dev.yml up --build`

#### UAT

1. Review `infra/app/env/uat.env.example`
2. Start the stack:
   - `docker compose -f infra/app/docker/docker-compose.uat.yml up --build`

#### PROD

1. Review `infra/app/env/prod.env.example`
2. Start the stack:
   - `docker compose -f infra/app/docker/docker-compose.prod.yml up --build -d`

Default service intent:

- DEV: API only, optional Redis, messaging disabled
- UAT: API + PostgreSQL + Redis + RabbitMQ + outbox worker + queue worker
- PROD: API + PostgreSQL + Redis + RabbitMQ + outbox worker + queue worker behind Nginx/ALB

## Database Migrations

`Vinayak` now includes an Alembic base for schema-managed deployments.

### Run the first migration

- `py -3 -m alembic -c alembic.ini upgrade head`

### Create a new migration later

- `py -3 -m alembic -c alembic.ini revision -m "describe_change"`

### Current migration files

- config: `alembic.ini`
- env: `app/vinayak/db/migrations/env.py`
- first revision: `vinayak/db/migrations/versions/0001_initial.py`
- outbox revision: `vinayak/db/migrations/versions/0002_outbox_events.py`

## Health and Readiness

`Vinayak` now exposes deployment-friendly endpoints for local checks, Docker health probes, and AWS load balancer targets.

- `GET /health`
- `GET /health/live`
- `GET /health/ready`

Readiness includes:

- database connectivity status
- document store readiness summary
- cache readiness summary
- message bus readiness summary
- broker credential readiness summary

## Demo Notes

- For local/demo use, `VINAYAK_DATABASE_URL` can stay on SQLite.
- For UAT/PROD, use PostgreSQL.
- MongoDB stays optional until a real document-data use case appears.
- Live Dhan routing requires:
  - `DHAN_CLIENT_ID`
  - `DHAN_ACCESS_TOKEN`
  - `DHAN_SECURITY_MAP`

## Next Hardening Steps

1. Move secrets to AWS Systems Manager or Secrets Manager.
2. Put Nginx or ALB in front of the app for production ingress.
3. Add CI to run tests and migrations before deployment.
4. Add ECS or EC2 deployment automation for AWS rollout.

## Live Analysis API

Vinayak can now reuse the current Trading project's live OHLCV and strategy workflow through the dashboard API.

- `GET /dashboard/candles` returns live candle rows for a symbol, interval, and period.
- `POST /dashboard/live-analysis` fetches live candles and runs the Trading workflow strategy layer inside Vinayak.
- Live candle fetch now supports Redis hot-cache, file-cache fallback, and CSV fallback.
- Live analysis now writes local report artifacts and can also push them to S3 when `REPORTS_S3_BUCKET` is configured.

Example payload:

```json
{
  "symbol": "^NSEI",
  "interval": "5m",
  "period": "1d",
  "strategy": "Breakout",
  "capital": 100000,
  "risk_pct": 1,
  "rr_ratio": 2,
  "trailing_sl_pct": 0.5,
  "strike_step": 50,
  "moneyness": "ATM",
  "strike_steps": 0,
  "mtf_ema_period": 3,
  "mtf_setup_mode": "either",
  "mtf_retest_strength": true,
  "mtf_max_trades_per_day": 3
}
```

Relevant production env vars:

- `REDIS_URL`
- `REPORTS_DIR`
- `REPORTS_S3_BUCKET`
- `REPORTS_S3_PREFIX`
- `AWS_REGION`
- `YFINANCE_TIMEOUT`

## Selected AWS Production Target

The chosen target for Vinayak is now the full managed AWS path:

- Route 53
- ACM
- ALB
- ECS Fargate
- RDS PostgreSQL
- ElastiCache Redis
- S3
- Secrets Manager
- CloudWatch

Deployment blueprint files are now under:

- `infra/app/aws/README.md`
- `infra/app/aws/aws_target_architecture.md`
- `infra/app/aws/ecs-task-definition.json`
- `infra/app/aws/ecs-service-vars.env.example`

This is now the preferred production direction for Vinayak instead of the earlier EC2-first temporary path.




