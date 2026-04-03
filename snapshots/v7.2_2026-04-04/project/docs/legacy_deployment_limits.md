# Legacy Deployment Limits

Scope: current deployment boundaries for the legacy runtime.

## Current Position

The legacy runtime supports local and operator-led deployment paths today.
Production-style deployment assets exist in the repository, but they should be treated as non-default or aspirational surfaces until runtime hardening is complete.

## Supported Now

- local Streamlit run via `src/Trading.py`
- local launcher via `tools/run_app.ps1`
- local Docker-oriented experimentation via `deploy/docker/`

## Not a Current Legacy Production Contract

Do not treat these as evidence of a fully hardened production profile:
- `deploy/k8s/`
- `deploy/aws/`
- reverse-proxy / public-domain examples
- cloud scaling assumptions beyond the current single-runtime legacy architecture

## Why The Limit Exists

The legacy runtime still carries monolithic coupling across:
- UI and operator workflow control
- execution and broker routing
- local/runtime artifact management
- mixed local and deployment assumptions

The repo also still contains reference and historical surfaces that can be mistaken for deployable targets unless operators stay within the active-surface contract.

## Required Active Deployment Surface

If you need a supported current-state target, use only:
- `src/Trading.py`
- `tools/run_app.ps1`
- `deploy/docker/Dockerfile`
- `deploy/docker/docker-compose.yml`

## Reference Docs

- `docs/active_code_surface.md`
- `docs/aws_deployment.md`
- `src/EXPERIMENTAL_SURFACE.md`
- `src/_archive/README.md`
- `snapshots/README.md`
