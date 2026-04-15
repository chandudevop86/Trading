param(
    [switch]$Once,
    [int]$PollSeconds = 120
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
$env:MARKET_DATA_PROVIDER = 'YAHOO'
$env:TRADING_BROKER_MODE = 'PAPER'
$env:LIVE_TRADING_ENABLED = 'false'

function Get-DotEnvValue {
    param(
        [string]$Path,
        [string]$Key
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return ''
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = [string]($line).Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed)) { continue }
        if ($trimmed.StartsWith('#')) { continue }
        if (-not $trimmed.StartsWith("$Key=")) { continue }
        return $trimmed.Substring($Key.Length + 1).Trim()
    }

    return ''
}

$dotenvPath = Join-Path $repoRoot '.env'
$telegramToken = ''
if (-not [string]::IsNullOrWhiteSpace($env:TELEGRAM_BOT_TOKEN)) {
    $telegramToken = $env:TELEGRAM_BOT_TOKEN.Trim()
}
elseif (Test-Path -LiteralPath $dotenvPath) {
    $telegramToken = Get-DotEnvValue -Path $dotenvPath -Key 'TELEGRAM_BOT_TOKEN'
}

$telegramChatId = ''
if (-not [string]::IsNullOrWhiteSpace($env:TELEGRAM_CHAT_ID)) {
    $telegramChatId = $env:TELEGRAM_CHAT_ID.Trim()
}
elseif (Test-Path -LiteralPath $dotenvPath) {
    $telegramChatId = Get-DotEnvValue -Path $dotenvPath -Key 'TELEGRAM_CHAT_ID'
}

$telegramEnabled = (-not [string]::IsNullOrWhiteSpace($telegramToken)) -and (-not [string]::IsNullOrWhiteSpace($telegramChatId))

$commonArgs = @(
    '-3',
    '-m', 'src.auto_run',
    '--execution-type', 'PAPER',
    '--allow-paper-on-fail',
    '--symbol', '^NSEI',
    '--interval', '5m',
    '--period', '5d',
    '--capital', '100000',
    '--risk-pct', '1.0',
    '--rr-ratio', '2.0',
    '--execution-symbol', 'NIFTY'
)

if ($telegramEnabled) {
    $commonArgs += @(
        '--send-telegram',
        '--telegram-token', $telegramToken,
        '--telegram-chat-id', $telegramChatId
    )
}

function Invoke-PaperRun {
    Write-Host ('Starting legacy paper auto-run at {0}' -f (Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))
    Write-Host ('Market data provider: {0}' -f $env:MARKET_DATA_PROVIDER)
    if ($telegramEnabled) {
        Write-Host 'Telegram summary: ENABLED'
    }
    else {
        Write-Host 'Telegram summary: DISABLED'
    }
    & py @commonArgs
    $script:lastRunExitCode = $LASTEXITCODE
    Write-Host ('Legacy paper auto-run finished with exit code {0}' -f $script:lastRunExitCode)
}

if ($Once) {
    Invoke-PaperRun
    exit $script:lastRunExitCode
}

Write-Host 'Legacy paper suite running in continuous paper observation mode.'
Write-Host ('Polling interval: {0} seconds' -f $PollSeconds)
Write-Host 'Press Ctrl+C in this window to stop the legacy paper suite.'

while ($true) {
    Invoke-PaperRun
    Write-Host ('Sleeping for {0} seconds before next cycle...' -f $PollSeconds)
    Start-Sleep -Seconds $PollSeconds
}


