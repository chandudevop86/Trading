# Intratrade Dashboard Fix Report

Generated: 2026-03-16 (IST)

## Problem
Dashboard summary was misleading:

- `Intratrade alert` showed many rows (e.g., `Trades: 103`) but `Win rate: 0.00%`, `Total PnL: 0.00`, `Last exit: N/A`, `Last reason: N/A`.

Root cause: the dashboard was sending **non-closed rows** (signals / indicator rows / mixed rows) into the **closed-trade** summarizer.

## Fix Location
- `F:\Trading\src\telegram_notifier.py` → `build_trade_summary()` (around line 12).

## What I Changed (Steps)
1. **Classify input rows** before summarizing:
   - Indicator rows: contain `market_signal`
   - Execution candidates: contain `signal_time` + `side`
   - Trade signals (entries without exits): contain `entry_price/stop_loss/target_price` but no `pnl/exit_*`
2. **Only compute Win rate / PnL on closed trades**:
   - Filter to rows that contain `pnl` and at least one of `exit_time` / `exit_reason`.
3. **Fallback summaries** for signals:
   - If there are no closed trades, show a `Signal alert` with last time/side/entry/SL/target.

## How To Export To PDF
Option A (recommended):
1. Open this file: `F:\Trading\intratrade_fix_report.html`
2. Press **Ctrl+P** → choose **Save as PDF**.

Option B:
- Open `F:\Trading\intratrade_fix_report.md` and print to PDF from your editor.
