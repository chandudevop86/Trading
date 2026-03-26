# Legacy Trading Runtime

This repository still contains multiple generations of assets, but the current supported legacy runtime is the monolithic application under `src/`.

## Start Here

Current-state docs are split by purpose:
- Local/operator guidance: `docs/legacy_local_ops.md`
- Deployment boundaries and limits: `docs/legacy_deployment_limits.md`
- Migration direction and guardrails: `docs/migration_notes.md`
- Active supported runtime surface: `docs/active_code_surface.md`
- Experimental/reference surfaces: `src/EXPERIMENTAL_SURFACE.md`, `src/_archive/README.md`, `snapshots/README.md`

## Supported Legacy Entrypoints

- `streamlit run src/Trading.py`
- `tools/run_app.ps1`
- `py -3 -m src.breakout_bot ...`
- `py -3 -m src.auto_run ...`
- `py -3 -m src.auto_backtest ...`
- `py -3 -m src.dhan_example ...`
- `py -3 -m src.dhan_account ...`

## Generic Rules Utilities

- `py -3 -m src.main --input data/input.csv --rules data/rules.yaml --output data/output.csv`
- `py -3 -m src.nifty50 --snapshot-output data/nifty50_snapshot.csv --rules data/nifty50_rules.yaml --scored-output data/nifty50_scored.csv`
- `py -3 -m src.nifty_options --input data/nifty_options_chain_sample.csv --rules data/nifty_options_rules.yaml --output data/nifty_options_scored.csv`
- `py -3 -m src.nifty_futures --symbol NIFTY --snapshot-output data/nifty_futures_snapshot.csv --rules data/nifty_futures_rules.yaml --scored-output data/nifty_futures_scored.csv`

## Quick Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
