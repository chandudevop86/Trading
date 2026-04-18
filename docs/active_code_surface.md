# Active Code Surface

Supported runtime and operator surface:
- API runtime: `py -3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000`
- Application package: `app/vinayak/`
- Docker API runtime: `infra/app/docker/Dockerfile`
- Production systemd runtime: `infra/production/systemd/vinayak-api.service`

Supported code boundaries:
- API: `app/vinayak/api/`
- Application orchestration and services: `app/vinayak/api/services/`, `app/vinayak/services/`, `app/vinayak/execution/`, `app/vinayak/market_data/`
- Persistence and infrastructure: `app/vinayak/db/`, `app/vinayak/cache/`, `app/vinayak/messaging/`, `app/vinayak/observability/`, `app/vinayak/infrastructure/`

Deprecated runtime surface:
- `src/` entrypoints and operators
- `streamlit run src/Trading.py`
- `py -3 -m src.auto_run`
- `py -3 -m src.auto_backtest`

Reference-only surfaces that are not supported runtime or deployment targets:
- `src/_archive/`
- `snapshots/`
- backup/temp files documented in `src/EXPERIMENTAL_SURFACE.md`

If a file or directory is not listed above as an active surface, do not treat it as an approved operator or deployment target.
