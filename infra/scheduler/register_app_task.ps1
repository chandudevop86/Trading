param(
  [string]$TaskName = "IntratradeAppAutoStart",
  [string]$StartTime = "09:10"
)
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$psScript = Join-Path $repoRoot "tools\run_app.ps1"
$arg = "-NoProfile -ExecutionPolicy Bypass -File `"$psScript`""
C:\Windows\System32\schtasks.exe /Create /TN $TaskName /TR "powershell.exe $arg" /SC DAILY /ST $StartTime /F
C:\Windows\System32\schtasks.exe /Query /TN $TaskName /V /FO LIST