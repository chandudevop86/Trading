# Vinayak Migration Plan

## Phase Order
1. Freeze legacy `src` strategy and execution interfaces except for critical fixes.
2. Route all new feature work into `vinayak.domain`, `vinayak.market_data`, `vinayak.strategies`, and `vinayak.execution`.
3. Introduce `signals_v2`, `execution_requests_v2`, `executions_v2`, `positions_v2`, `validation_logs_v2`, `audit_logs_v2`, `backtest_reports_v2`, and `strategy_runs_v2` behind additive migrations.
4. Dual-write validation and audit events from the legacy flow into `*_v2` tables before cutting reads over.
5. Shift Streamlit to API-only mode and stop direct broker or repository access from UI code.
6. Move live execution to `ProductionExecutionService` plus Redis-backed `ExecutionGuard`.
7. Put market-data ingress behind `MarketDataService` normalization and cache.
8. Switch backtest runs to the shared `TradeSignal` contract and compare live-vs-backtest divergences for one full release window.
9. Migrate deployment to containerized API, UI, worker, Postgres, and Redis.
10. Remove the last `src` compatibility bridges after all route, test, and batch workloads import only `vinayak.*`.

## Temporary Compatibility Rules
- Keep the top-level `vinayak` shim until every entrypoint runs under `python -m vinayak...`.
- Keep the approved `src` bridges only in the legacy allowlist already enforced by package-layout tests.
- Do not add new `src` imports anywhere under `app/vinayak`.
- Leave legacy execution service paths read-only except for adapters that dual-write or translate into the new contracts.

## Rollback Guidance
- Roll back the route wiring first, not the contracts.
- Keep `*_v2` tables additive until at least one release cycle after cutover.
- If Redis guard logic causes false rejects, fall back to paper-only mode and keep audit logging on.
- If API cutover fails, keep Streamlit pointed at the legacy endpoints while preserving the new domain modules in place.
