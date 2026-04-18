# Temporary Legacy Adapters

`app/vinayak/legacy/` is not a primary runtime surface.

These modules exist only as temporary migration adapters for functionality that has not yet been fully reimplemented under app-owned domain/application/infrastructure modules. New code should not import from this package unless the dependency is being explicitly isolated as part of migration work.

## Current isolation status

- The supported FastAPI runtime does not import `vinayak.legacy.*`.
- The package remains only as a documented migration boundary while historical references are audited and retired.
- `src/` fallback behavior must not be reintroduced into active runtime files under `app/`.

## Remaining migration surface

- Historical compatibility tests and archived operator flows
- Deprecated `src/`-era documentation and deployment references
- Any ad hoc manual scripts that still assume `src/` layout
