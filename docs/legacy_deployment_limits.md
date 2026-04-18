# Archived Legacy Deployment Limits

This document is retained only as migration/reference history for the deprecated `src/` runtime.

## Status

The legacy deployment surface is not the supported current production contract.

Do not treat any of the following as active deployment targets:
- `src/Trading.py`
- `tools/run_app.ps1`
- deprecated Streamlit/systemd/docker paths for `src`

## Supported deployment surface

Use only the FastAPI deployment contract documented in:
- `README.md`
- `docs/active_code_surface.md`
- `DEPLOYMENT.md`
- `ROLLBACK.md`
- `docs/aws_deployment.md`

## Why this file still exists

It helps explain historical repository references while the last archived `src` materials remain in the repo for migration traceability.
