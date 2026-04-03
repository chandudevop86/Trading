# Live Validation One-Page Checklist

Date: Monday, April 6, 2026

## Mode

Use paper only for automated entry and exit handling.

```text
execution_type=PAPER
auto_execute=true
kill_switch_enabled=true
max_trades_per_day=1
risk_pct=very small
max_daily_loss=small fixed amount
```

## Before Open

1. Confirm DB is reachable
2. Confirm dashboard opens
3. Confirm observability page opens
4. Confirm market data is fresh
5. Confirm Telegram test works
6. Confirm no red alerts
7. Confirm strategy is fixed for the day
8. Confirm paper mode only

## Watch These

1. `execution_attempt_total`
2. `execution_success_total`
3. `execution_failed_total`
4. `execution_blocked_total`
5. `duplicate_execution_block_total`
6. `portfolio_kill_switch_active`
7. `pnl_today`
8. `active_alerts_total`

## Trading Rule

1. One strategy only
2. One symbol group only
3. One trade max for Monday
4. Let auto-run handle paper entry and paper exit only
5. Do not switch to live during the session

## Stop Immediately If

1. `execution_failed_total` increases unexpectedly
2. Duplicate behavior appears
3. Data becomes stale
4. Kill switch activates
5. Unexplained red alert appears

## After Close

1. Review execution summary
2. Review blocked and failed counts
3. Review alerts
4. Record PASS or PAUSE for Tuesday
