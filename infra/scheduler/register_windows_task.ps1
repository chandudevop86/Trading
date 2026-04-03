param(
    [string]$TaskName = "IntratradeAutoBacktest",
    [string]$Schedule = "MINUTE",
    [int]$Modifier = 15,
    [string]$StartTime = "09:15"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$psScript = Join-Path $repoRoot "tools\run_auto_backtest.ps1"
$arg = "-NoProfile -ExecutionPolicy Bypass -File `"$psScript`""

C:\Windows\System32\schtasks.exe /Create /TN $TaskName /TR "powershell.exe $arg" /SC $Schedule /MO $Modifier /ST $StartTime /F
C:\Windows\System32\schtasks.exe /Query /TN $TaskName /V /FO LIST