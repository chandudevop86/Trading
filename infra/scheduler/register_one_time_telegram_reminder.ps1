param(
    [string]$TaskName = "TradingOneTimeTelegramReminder",
    [Parameter(Mandatory = $true)]
    [string]$RunDate,
    [Parameter(Mandatory = $true)]
    [string]$RunTime,
    [string]$Message = "Trading cleanup reminder",
    [string]$Token = "",
    [string]$ChatId = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$psScript = Join-Path $repoRoot "deploy\scheduler\send_telegram_reminder.ps1"

if (-not (Test-Path $psScript)) {
    throw "Reminder script not found: $psScript"
}

$escapedMessage = $Message.Replace('"', '""')
$arg = "-NoProfile -ExecutionPolicy Bypass -File `"$psScript`" -Message `"$escapedMessage`""
if ($Token) {
    $arg += " -Token `"$Token`""
}
if ($ChatId) {
    $arg += " -ChatId `"$ChatId`""
}

C:\Windows\System32\schtasks.exe /Create /TN $TaskName /TR "powershell.exe $arg" /SC ONCE /SD $RunDate /ST $RunTime /F
C:\Windows\System32\schtasks.exe /Query /TN $TaskName /V /FO LIST
