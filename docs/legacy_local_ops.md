# Archived Legacy Local Ops

This document is retained only as migration history.

## Status

The legacy local/operator runtime documented here is deprecated and not a supported current workflow.

Do not use:
- `streamlit run src/Trading.py`
- `tools/run_app.ps1`
- `py -3 -m src.auto_run ...`
- `py -3 -m src.auto_backtest ...`
- `py -3 -m src.breakout_bot ...`

## Supported replacement

Use the FastAPI runtime documented in:
- `README.md`
- `docs/active_code_surface.md`
- `DEPLOYMENT.md`

Supported local entrypoint:
```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Reason this file still exists

It remains only to explain historical references that may still appear in archived docs, reports, snapshots, or old operational notes.
