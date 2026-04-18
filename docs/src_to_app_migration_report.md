# Src To App Migration Report

## Current Repo Problems

- Dual runtime surface: `src/` and `app/` both appear operator-supported in docs and tooling.
- Packaging ambiguity: `app/main.py` previously relied on `sys.path` mutation.
- Legacy dependency bleed: active market-data runtime paths still depended on `src`-era helpers.
- Execution ambiguity: multiple execution services still exist and need final consolidation.
- Documentation drift: operators could still infer that `src` entrypoints were supported.

## Active Src Capabilities Still Requiring Migration Or Isolation

- Legacy operator CLIs and Streamlit UI under `src/`
- Monolithic strategy/runtime orchestration in `src/trading_runtime_service.py`
- Legacy backtest and auto-run flows
- Compatibility-only tests under `tests/integration/legacy/`

## App Destinations

- Runtime entrypoint: `app/main.py`
- API and request handling: `app/vinayak/api/`
- Execution lifecycle: `app/vinayak/execution/`
- Market data and normalization: `app/vinayak/api/services/live_ohlcv.py`, `app/vinayak/market_data/`, `app/vinayak/infrastructure/market_data/`
- Persistence and repositories: `app/vinayak/db/`
- Observability: `app/vinayak/observability/`

## Target Architecture

```text
app/
  main.py
  vinayak/
    api/
      routes/
      schemas/
      dependencies/
    application/
      use_cases/
      services/
      orchestration/
    domain/
      models/
      services/
      rules/
      ports/
    infrastructure/
      db/
      repositories/
      market_data/
      brokers/
      cache/
      messaging/
      observability/
    web/
    workers/
    config/
    tests_support/
```

## File Move / Rewrite Plan

1. Consolidate runtime entrypoint and packaging around `app.main:app`.
2. Retire active `src` dependencies from market-data, execution, and route paths.
3. Collapse execution orchestration onto one reviewed-trade and execution workflow.
4. Move typed orchestration into `application/*` and keep API handlers thin.
5. Retire or isolate explicit legacy adapters after parity verification.
6. Remove deprecated `src` operator surfaces from supported docs and deployment assets.

## This Migration Slice

- Replaced the `app.main` import hack with a clean package-relative entrypoint.
- Added app-owned Dhan instrument/security-map loading under `app/vinayak/infrastructure/market_data/`.
- Extended the app Dhan broker client so live OHLCV retrieval no longer depends on `src.dhan_api`.
- Refactored live OHLCV fetching to prefer app-owned Yahoo and Dhan paths only.
- Replaced trading-workspace live security-map loading with the app execution payload loader.
- Moved active OHLCV preprocessing onto `app/vinayak/infrastructure/market_data/processing.py`.
- Moved active NSE option-chain parsing and metrics mapping onto `app/vinayak/infrastructure/market_data/option_chain.py`.
- Updated docs so `app/` is the active code surface and `src/` is marked deprecated.

## Remaining Gaps

- Production execution still exposes both `ExecutionService` and `ProductionExecutionService`.
- Some explicit `app/vinayak/legacy/*` adapters remain, but the supported runtime no longer depends on them for active data-preparation or option-metrics paths.
- Legacy tests and docs still exist for historical validation and need a separate deprecation sweep.
- Full `src/` retirement requires migrating or deleting the old Streamlit/operator surfaces after equivalent `app/` workflows are confirmed.
