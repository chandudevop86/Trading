# Vinayak Final Migration Checklist

- Confirm every new import path uses `vinayak.*` and no new `src.*` imports were introduced.
- Apply Alembic migration `0008_production_trading_v2` in a non-production environment first.
- Provision PostgreSQL and Redis before enabling `ProductionExecutionService`.
- Wire a real Redis-backed `GuardStateStore`; do not use the in-memory guard outside tests or single-process development.
- Configure `VINAYAK_DATABASE_URL`, `VINAYAK_REDIS_URL`, `VINAYAK_ADMIN_USERNAME`, `VINAYAK_ADMIN_PASSWORD`, and `VINAYAK_ADMIN_SECRET` in secrets management.
- Switch Streamlit to call only FastAPI endpoints and remove any residual repository or broker access from UI code.
- Dual-write audit and validation events into `audit_logs_v2` and `validation_logs_v2` before cutting admin reads over.
- Validate one strategy at a time against the `TradeSignal` contract and reject raw dict outputs.
- Run route, normalizer, guard, and backtest consistency tests in CI before enabling live mode.
- Keep live mode locked until broker adapter, unlock policy, and kill-switch checks are implemented.
- Set Kubernetes readiness to `/health/ready` and liveness to `/health`.
- Set autoscaling only on stateless API pods; never autoscale stateful broker sessions blindly.
- Confirm idempotency keys are stable across retries for signal runs and execution requests.
- Capture Prometheus metrics and structured logs before the first paper-trading cutover.
- Remove `src` shims only after CLI, API, worker, and tests all run via `vinayak`.
