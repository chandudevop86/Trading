param(
    [Parameter(Mandatory = $false)]
    [string]$Message = "Trading reminder",
    [Parameter(Mandatory = $false)]
    [string]$Token = "",
    [Parameter(Mandatory = $false)]
    [string]$ChatId = ""
)

$ErrorActionPreference = "Stop"

if (-not $Token) {
    $Token = [Environment]::GetEnvironmentVariable("TELEGRAM_TOKEN", "Process")
}
if (-not $Token) {
    $Token = [Environment]::GetEnvironmentVariable("TELEGRAM_TOKEN", "User")
}
if (-not $Token) {
    $Token = [Environment]::GetEnvironmentVariable("TELEGRAM_TOKEN", "Machine")
}

if (-not $ChatId) {
    $ChatId = [Environment]::GetEnvironmentVariable("TELEGRAM_CHAT_ID", "Process")
}
if (-not $ChatId) {
    $ChatId = [Environment]::GetEnvironmentVariable("TELEGRAM_CHAT_ID", "User")
}
if (-not $ChatId) {
    $ChatId = [Environment]::GetEnvironmentVariable("TELEGRAM_CHAT_ID", "Machine")
}

if (-not $Token) {
    throw "Telegram token is required. Pass -Token or set TELEGRAM_TOKEN."
}
if (-not $ChatId) {
    throw "Telegram chat id is required. Pass -ChatId or set TELEGRAM_CHAT_ID."
}
if (-not $Message.Trim()) {
    throw "Reminder message cannot be empty."
}

$uri = "https://api.telegram.org/bot$Token/sendMessage"
$body = @{
    chat_id = $ChatId
    text = $Message
}

$response = Invoke-RestMethod -Method Post -Uri $uri -Body $body -ContentType 'application/x-www-form-urlencoded'
$response | ConvertTo-Json -Depth 6
