#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${REPO_DIR:-/opt/intratrade}"
SYMBOL="${SYMBOL:-RELIANCE.NS}"
INTERVAL="${INTERVAL:-5m}"
PERIOD="${PERIOD:-5d}"
EXEC_SYMBOL="${EXEC_SYMBOL:-RELIANCE}"

cd "$REPO_DIR"
py -3 -m src.auto_backtest \
  --symbol "$SYMBOL" \
  --interval "$INTERVAL" \
  --period "$PERIOD" \
  --execution-symbol "$EXEC_SYMBOL"