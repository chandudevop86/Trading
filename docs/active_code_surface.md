# Active Code Surface

Supported current-state legacy operator surface:
- UI: `streamlit run src/Trading.py`
- Local launcher: `tools/run_app.ps1`
- Docker local profile: `deploy/docker/Dockerfile` and `deploy/docker/docker-compose.yml`
- Batch/operator CLIs under active support: `src.auto_run`, `src.auto_backtest`, `src.breakout_bot`, `src.dhan_example`, `src.dhan_account`

Reference-only surfaces that are not supported runtime or deployment targets:
- `src/breakout_app.py` compatibility wrapper
- backup/temp files documented in `src/EXPERIMENTAL_SURFACE.md`
- `src/_archive/`
- `snapshots/`
- rewrite/parallel platform surfaces outside the supported legacy runtime boundary

If a file or directory is not listed above as an active surface, do not treat it as an approved operator or deployment target.
