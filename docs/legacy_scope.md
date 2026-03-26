# Legacy Product Scope

Date: 2026-03-26
Scope owner: Legacy monolithic trading application under `F:\Trading\src`

## Purpose

This document defines the official product boundary for the maintained legacy trading system so migration and maintenance work do not drift across multiple codebase generations.

## Legacy Product Definition

The supported legacy product in this repository is the monolithic trading application implemented under `src/`.

This means:

- `src/` is the maintained legacy runtime
- `tests/` is the legacy regression and baseline verification surface
- `data/` and `logs/` are the legacy runtime artifact locations
- legacy-facing documentation should point to the supported `src/` entrypoints only

## In Scope for Legacy Maintenance

The following areas are part of the supported legacy product boundary:

- `F:\Trading\src`
- `F:\Trading\tests`
- `F:\Trading\data`
- `F:\Trading\logs`
- `F:\Trading\README.md` for legacy run guidance
- `F:\Trading\docs\legacy_src_contracts.md`
- deploy and tooling assets that directly support the `src/` runtime path

## Out of Scope for Legacy Runtime Ownership

The following areas are not part of the maintained legacy runtime boundary:

- `F:\Trading\vinayak`
  This is the next-generation platform direction, not the legacy monolith.

- `F:\Trading\snapshots`
  These are rollback, archive, or reference artifacts.

- `F:\Trading\src\_archive`
  These are archived legacy files and should not be treated as supported runtime entrypoints.

- historical or duplicate experimental files outside the supported `src/` entrypoints
- `F:\Trading\src\pattern_detector.py` and `F:\Trading\src\operational_daemon.py` as blocked non-canonical direct launchers

## Supported Legacy Entrypoints

The supported legacy runtime surface is:

- UI operator console: `streamlit run src/Trading.py`
- Generic CSV rules CLI: `py -3 -m src.main ...`
- NIFTY 50 snapshot CLI: `py -3 -m src.nifty50 ...`
- NIFTY options rules CLI: `py -3 -m src.nifty_options ...`
- NIFTY futures CLI: `py -3 -m src.nifty_futures ...`
- Breakout CLI: `py -3 -m src.breakout_bot ...`
- BTST CLI: `py -3 -m src.btst_bot ...`
- End-to-end pipeline: `py -3 -m src.auto_run ...`
- Backtesting workflow: `py -3 -m src.auto_backtest ...`
- Dhan order preview/live CLI: `py -3 -m src.dhan_example ...`
- Dhan account CLI: `py -3 -m src.dhan_account ...`
- Reconciliation CLIs: `py -3 -m src.reconcile_live ...` and `py -3 -m src.reconcile_positions ...`

Blocked non-canonical direct launchers:

- `F:\Trading\src\breakout_app.py`
- `F:\Trading\src\pattern_detector.py`
- `F:\Trading\src\operational_daemon.py`

## Boundary Rules

During legacy maintenance:

- bug fixes and stabilization work for the old app should target `src/`
- legacy docs should not point operators to `vinayak/`
- snapshots and archives should be treated as references, not supported runtime surfaces
- new migration work may read from the legacy baseline, but should not blur ownership of the old app

## Why This Boundary Matters

Without an explicit scope boundary, the repository can be misread as one active product surface even though it currently contains:

- the maintained monolith
- the next-generation rewrite track
- snapshots
- archived files
- duplicate historical assets

This document reduces that ambiguity and gives Phase 0 a stable ownership line.
