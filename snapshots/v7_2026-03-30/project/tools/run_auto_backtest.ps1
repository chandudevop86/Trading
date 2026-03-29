param(
    [string]$Symbol = "RELIANCE.NS",
    [string]$Interval = "5m",
    [string]$Period = "5d",
    [string]$ExecutionSymbol = "RELIANCE",
    [double]$Capital = 100000,
    [double]$RiskPct = 0.01,
    [double]$RrRatio = 2.0,
    [int]$PivotWindow = 2,
    [string]$EntryCutoff = "11:30"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

& py -3 -m src.auto_backtest `
    --symbol $Symbol `
    --interval $Interval `
    --period $Period `
    --execution-symbol $ExecutionSymbol `
    --capital $Capital `
    --risk-pct $RiskPct `
    --rr-ratio $RrRatio `
    --pivot-window $PivotWindow `
    --entry-cutoff $EntryCutoff