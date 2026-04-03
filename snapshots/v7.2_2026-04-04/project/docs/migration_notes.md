# Migration Notes

Scope: pointers for future migration work without redefining the current legacy operating surface.

## Current Baseline

The maintained legacy product is the `src/` monolith and its active operator/runtime entrypoints.
Migration work should preserve that baseline while reducing coupling and operational ambiguity.

## Current Refactor Direction

The codebase is being separated by domain:
- market/data helpers
- strategies and routing
- execution workflows
- reporting/summary helpers
- UI/presentation helpers

Recent examples include:
- `src/trading_runtime_service.py`
- `src/trading_ui_service.py`
- `src/market_data_service.py`
- `src/reporting_service.py`
- `src/runtime_file_service.py`

## Migration Guardrails

- Do not expand the active legacy runtime surface while migrating.
- Do not point operators to `snapshots/`, `src/_archive/`, or compatibility wrappers.
- Keep current-state docs aligned with the supported legacy entrypoints.
- Treat rewrite or next-generation directions as separate from the maintained legacy runtime.

## Related Context Docs

- `docs/legacy_scope.md`
- `docs/legacy_src_contracts.md`
- `docs/legacy_src_gap_analysis.md`
- `docs/monolith_to_microservices_migration_report.md`
- `docs/trading_platform_transformation_proposal.md`
