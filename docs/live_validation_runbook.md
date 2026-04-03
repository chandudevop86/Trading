# Live Validation Runbook

Date: Monday, April 6, 2026

## Session Goal

Validate that live order flow, risk controls, and alerts behave correctly with minimal capital.

Success means:

1. No duplicate orders
2. No wrong quantity, symbol, or side
3. No unexplained execution failures
4. Clean broker-to-DB reconciliation

## Config

Set these before market open:

```text
execution_type=LIVE
auto_execute=true
kill_switch_enabled=true
max_trades_per_day=1
risk_pct=very small
max_daily_loss=small fixed amount
```

Use:

1. One strategy only
2. One symbol group only
3. One broker only
4. Smallest live size possible

## Pre-Market Checklist

Run these checks before enabling live trading:

1. Broker credentials loaded
2. Security map path correct
3. Database reachable
4. Admin dashboard opens
5. Observability page opens
6. Telegram test message succeeds
7. Latest market data is fresh
8. No red alerts are active
9. Kill switch metric is `false` before start
10. Strategy selected is the one you tested most

## Must-Watch Metrics

Keep these visible:

1. `execution_attempt_total`
2. `execution_success_total`
3. `execution_failed_total`
4. `execution_blocked_total`
5. `duplicate_execution_block_total`
6. `portfolio_kill_switch_active`
7. `pnl_today`
8. `active_alerts_total`

## Live Execution Rules

1. Do not allow more than one live trade for Monday.
2. After the first order, pause and verify everything before doing anything else.
3. Verify:

broker reference present
correct symbol
correct side
correct quantity
correct execution status
correct reviewed trade linkage

## Immediate Stop Conditions

Stop live trading for the day if any one happens:

1. Wrong order quantity
2. Wrong symbol
3. Wrong side
4. Duplicate order
5. `execution_failed_total` increases unexpectedly
6. Stale market data
7. Kill switch activates
8. Unexplained red alert appears

## If First Trade Executes

Check:

1. Order appears at broker
2. Broker reference stored in DB
3. Execution row status is correct
4. No duplicate blocked or failed spike
5. Alerts remain understandable

If all five are clean, Monday is a pass for phase 1 validation.

## Post-Market Checklist

After close:

1. Reconcile broker orders with execution records
2. Confirm every execution has broker reference
3. Review blocked and failed counts
4. Review alert history
5. Record:

strategy used
signal count
trade count
PnL
issues seen
go/no-go for Tuesday

## Decision Rule

1. If Monday is clean: keep same size on Tuesday, April 7, 2026
2. If any unexplained issue occurs: pause live trading and fix before next session

## Operator Log Template

```text
Date: 2026-04-06
Strategy:
Symbol Group:
Execution Mode: LIVE
Max Trades: 1
Risk %:
Max Daily Loss:

Pre-market checks:
[ ] Broker ready
[ ] DB ready
[ ] Market data fresh
[ ] Dashboard ready
[ ] Telegram ready
[ ] No red alerts

Trade result:
Signal seen:
Order placed:
Broker reference:
Status:
Quantity correct:
Symbol correct:
Side correct:

Metrics:
execution_attempt_total:
execution_success_total:
execution_failed_total:
execution_blocked_total:
duplicate_execution_block_total:
pnl_today:
portfolio_kill_switch_active:

Incidents:
-

End-of-day verdict:
[ ] PASS
[ ] PAUSE
Notes:
```
