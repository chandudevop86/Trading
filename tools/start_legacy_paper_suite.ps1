param(
    [switch]$Once,
    [int]$PollSeconds = 120
)

$ErrorActionPreference = 'Stop'
Set-Location 'F:\Trading'
$env:MARKET_DATA_PROVIDER = 'YAHOO'
$env:TRADING_BROKER_MODE = 'PAPER'
$env:LIVE_TRADING_ENABLED = 'false'

$commonArgs = @(
    '-3',
    '-m', 'src.auto_run',
    '--execution-type', 'PAPER',
    '--symbol', '^NSEI',
    '--interval', '5m',
    '--period', '5d',
    '--capital', '100000',
    '--risk-pct', '1.0',
    '--rr-ratio', '2.0',
    '--execution-symbol', 'NIFTY'
)

function Invoke-PaperRun {
    Write-Host ('Starting legacy paper auto-run at {0}' -f (Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))
    Write-Host ('Market data provider: {0}' -f $env:MARKET_DATA_PROVIDER)
    & py @commonArgs
    $script:lastRunExitCode = $LASTEXITCODE
    Write-Host ('Legacy paper auto-run finished with exit code {0}' -f $script:lastRunExitCode)
}

if ($Once) {
    Invoke-PaperRun
    exit $script:lastRunExitCode
}

Write-Host 'Legacy paper suite running in continuous paper mode.'
Write-Host ('Polling interval: {0} seconds' -f $PollSeconds)
Write-Host 'Press Ctrl+C in this window to stop the legacy paper suite.'

while ($true) {
    Invoke-PaperRun
    Write-Host ('Sleeping for {0} seconds before next cycle...' -f $PollSeconds)
    Start-Sleep -Seconds $PollSeconds
}

