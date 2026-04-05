# Active Code Surface

Supported current-state legacy operator surface:
- UI: `streamlit run src/Trading.py`
- Local launcher: `tools/run_app.ps1`
- Docker local profile: `deploy/docker/Dockerfile` and `deploy/docker/docker-compose.yml`
- Batch/operator CLIs under active support: `src.auto_run`, `src.auto_backtest`, `src.breakout_bot`, `src.dhan_example`, `src.dhan_account`

Compatibility-supported but not primary operator surface:
- `src.nifty50` -> prefer `tools/run_auto_backtest.ps1` when the workflow is batch-oriented
- `src.nifty_options` -> prefer `streamlit run src/Trading.py`
- `src.nifty_futures` -> prefer `streamlit run src/Trading.py`
- `src.btst_bot` -> prefer `py -3 -m src.auto_backtest`

Deprecated legacy operator surface:
- `src.main` -> use `py -3 -m src.nifty50`
- `src.reconcile_live` -> use `py -3 -m src.auto_run`
- `src.reconcile_positions` -> use `py -3 -m src.auto_run`

Use compatibility-supported entrypoints only to preserve older scripts or operator habits. New automation and new documentation should target the active surface first. Deprecated entrypoints should be removed from operator usage entirely.

Reference-only surfaces that are not supported runtime or deployment targets:
- `src/breakout_app.py` compatibility wrapper
- backup/temp files documented in `src/EXPERIMENTAL_SURFACE.md`
- `src/_archive/`
- `snapshots/`
- rewrite/parallel platform surfaces outside the supported legacy runtime boundary

If a file or directory is not listed above as an active surface or compatibility-supported surface, do not treat it as an approved operator or deployment target.
