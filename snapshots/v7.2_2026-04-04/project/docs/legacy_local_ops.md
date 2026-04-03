# Legacy Local Ops

Scope: current supported local/operator guidance for the legacy runtime.

## Supported Local Entrypoints

- UI: `streamlit run src/Trading.py`
- Local launcher: `tools/run_app.ps1`
- Breakout CLI: `py -3 -m src.breakout_bot ...`
- Backtest workflow: `py -3 -m src.auto_backtest ...`
- End-to-end operator pipeline: `py -3 -m src.auto_run ...`
- Broker/account utilities: `py -3 -m src.dhan_example ...` and `py -3 -m src.dhan_account ...`

## Local Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Local UI Run

```bash
streamlit run src/Trading.py
```

What the local UI supports now:
- operator-led symbol / timeframe / strategy selection
- trade generation
- backtest initiation
- paper execution flow
- live-routing checks when explicitly enabled

## Local AWS-Optional Flow

1. Install dependencies:
   - `py -3 -m pip install -r requirements.txt`
2. Configure local AWS credentials if you want S3 mirroring.
3. Run the UI locally:
   - `streamlit run src/Trading.py`
4. Use AWS/S3-related options only as local operator features, not as evidence of a hardened production deployment profile.

## Local Runtime Artifacts

Primary runtime artifacts are written under:
- `data/`
- `logs/`

Current active-surface reference:
- `docs/active_code_surface.md`
