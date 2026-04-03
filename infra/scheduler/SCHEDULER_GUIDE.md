# Auto Backtest Scheduler Guide

This sets fully automatic backtest + paper logs without manual intervention.

## Output files updated each run
- `data/live_ohlcv.csv`
- `data/backtest_results_all.csv`
- `data/backtest_results_history.csv`
- `data/paper_trading_logs_all.csv`

## Windows Task Scheduler

### 1) Test manually once
```powershell
cd F:\Trading
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\run_auto_backtest.ps1
```

### 2) Register recurring task (every 15 minutes)
```powershell
cd F:\Trading
powershell -NoProfile -ExecutionPolicy Bypass -File .\deploy\scheduler\register_windows_task.ps1 -TaskName "IntratradeAutoBacktest" -Schedule MINUTE -Modifier 15 -StartTime "09:15"
```

### 3) Run on demand
```powershell
schtasks /Run /TN "IntratradeAutoBacktest"
```

### 4) Check status
```powershell
schtasks /Query /TN "IntratradeAutoBacktest" /V /FO LIST
```

### 5) Delete task
```powershell
schtasks /Delete /TN "IntratradeAutoBacktest" /F
```

## Linux Cron (if deployed on Linux)

### 1) Make runner executable
```bash
cd /opt/intratrade
chmod +x infra/scheduler/run_auto_backtest.sh
```

### 2) Add cron entry (every 15 minutes)
```bash
crontab -e
```
Add this line:
```cron
*/15 * * * * REPO_DIR=/opt/intratrade SYMBOL=RELIANCE.NS INTERVAL=5m PERIOD=5d EXEC_SYMBOL=RELIANCE /opt/intratrade/infra/scheduler/run_auto_backtest.sh >> /opt/intratrade/data/cron_backtest.log 2>&1
```

### 3) Verify cron installed
```bash
crontab -l
```

## Customize strategy feed
- Symbol: `RELIANCE.NS` / `^NSEI` / other Yahoo ticker
- Interval: `1m|2m|5m|15m|30m|60m|1d`
- Period: `1d|5d|1mo`

## Notes
- Uses live OHLCV fetch from Yahoo on each run.
- No manual CSV upload needed.
- NIFTY quantity enforcement remains in execution engine (lot multiple logic).
