# Migration Notes

Scope: migrate supported runtime behavior from `src/` into `app/` and retire `src/` from operator-facing runtime paths.

## Current Baseline

- Supported runtime entrypoint: `app.main:app`
- Active application package: `app/vinayak/`
- Legacy `src/`: deprecated and retained only while specific behavior is being migrated or verified

## Current Refactor Direction

The migration is converging on one canonical application path:
- market data through app-owned fetch and normalization services
- validation and execution through one reviewed-trade and execution workflow
- observability from repository-backed and app-owned services
- route handlers as thin orchestration surfaces only

## Migration Guardrails

- Do not reintroduce `src.*` imports into active runtime files under `app/`
- Do not use `sys.path` mutation or repo-root import hacks in supported entrypoints
- Keep `app.main:app` as the only supported runtime start target
- Keep live execution guarded and explicit while consolidating execution paths
- Prefer typed app services and repositories over dict-heavy legacy coupling

## Current Stage

This migration pass:
- removes the `app.main` path hack
- moves active live-OHLCV fetching and Dhan security-map handling onto app-owned modules
- moves active OHLCV normalization and option-chain helpers onto app-owned infrastructure modules
- routes reviewed-trade and execution APIs through one facade boundary
- adds request-id middleware and structured request logging to the FastAPI runtime
- updates Docker and CI so the supported surface is `app.main:app`

## Related Context Docs

- `docs/active_code_surface.md`
- `docs/src_to_app_migration_report.md`
- `docs/monolith_to_microservices_migration_report.md`
- `docs/trading_platform_transformation_proposal.md`
