# Trading Platform Workspace

This repository now supports one runtime surface:

- the application tier under `app/`

The practical repo layout is now:

```text
app/      -> application tier package and ASGI entrypoint
web/      -> web-tier delivery assets such as nginx config
infra/    -> deployment and operations assets
docs/     -> architecture and runbooks
tests/    -> unit and integration test suites
src/      -> deprecated legacy source retained only for migration/reference
```

## Start Here

Current-state docs are split by purpose:
- Local/operator guidance: `docs/active_code_surface.md`
- Migration direction and guardrails: `docs/migration_notes.md`
- Migration inventory report: `docs/src_to_app_migration_report.md`
- Experimental/reference surfaces: `src/EXPERIMENTAL_SURFACE.md`, `src/_archive/README.md`, `snapshots/README.md`

## Supported Entrypoint

- `py -3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000`

`app.main:app` is the supported runtime entrypoint. Legacy `src/*` entrypoints are deprecated and should not be used for new operations, automation, or deployment.

## Migration Status

- Runtime entrypoint: `app.main:app`
- Active application package: `app/vinayak/`
- Legacy `src/`: deprecated and being retired behind explicit migration work only

## Quick Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
