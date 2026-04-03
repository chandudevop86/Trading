# Vinayak 3-Tier Documentation

Below is the communication between Vinayak components and their dependency flow. These dependencies come from the application design and current migration work. The infrastructure team can deploy them separately, but the functional dependency order stays the same.

![Vinayak 3-Tier Architecture](/F:/Trading/docs/vinayak_app_reference/vinayak_3tier_architecture.svg)

## WEB TIER

Usually the web tier is the layer that exposes frontend pages and receives user traffic.

In Vinayak, this tier contains:

- browser-facing pages like `/admin` and `/workspace`
- reverse proxy entry such as Nginx or AWS ALB
- static asset delivery and request forwarding to the API tier

Current mapping:

- `vinayak/web/app/main.py`
- `vinayak/web/app/workspace_html.py`

## APP TIER

The app tier contains backend code, APIs, business rules, strategy execution, integration logic, and notification flow.

In Vinayak, this tier contains:

- FastAPI routes under `vinayak/api/routes`
- strategy processing in `vinayak/api/services/trading_workspace.py`
- live market data processing in `vinayak/api/services/live_ohlcv.py`
- execution flow, Telegram integration, and report generation

Current mapping:

- `vinayak/api/routes/dashboard.py`
- `vinayak/api/services/trading_workspace.py`
- `vinayak/api/services/live_ohlcv.py`
- legacy strategy/execution integrations under `src/`

## DATA TIER

The data tier stores the application state and acceleration layers.

In Vinayak, this tier contains:

- relational storage for signals, reviewed trades, and executions
- Redis for hot cache and recent analysis artifacts
- S3 or local report storage for exports and summaries
- Secrets Manager target for runtime credentials

Current mapping:

- `vinayak/db/`
- `vinayak/cache/redis_client.py`
- `vinayak/api/services/report_storage.py`
- `vinayak/data/`

## Current Status

Vinayak now satisfies the 3-tier logic in structure:

- user traffic enters through the web tier
- the app tier is the only layer handling strategy and execution logic
- the data tier is separated for persistence, cache, and report artifacts

What still remains for full production maturity:

- live deployment validation on AWS
- real RDS, ElastiCache, and S3 environment verification
- secret rotation through AWS Secrets Manager
- CI/CD enforcement for migrations and tests

