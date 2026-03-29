$ErrorActionPreference = 'Stop'

$patterns = @(
    'src.operational_daemon',
    'streamlit run src\\Trading.py',
    'uvicorn vinayak.api.main:app',
    'start_legacy_paper_suite.ps1',
    'start_trading_ui_suite.ps1'
)

$targets = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -match 'powershell(\.exe)?|pwsh(\.exe)?' -and ($patterns | Where-Object { $_ -and $_.Length -gt 0 -and $_.CommandLine -like "*$_*" })
}

if (-not $targets) {
    Write-Host 'No matching trading stack PowerShell processes found.'
    exit 0
}

$targets | ForEach-Object {
    Write-Host ("Stopping PID {0}: {1}" -f $_.ProcessId, $_.CommandLine)
    Stop-Process -Id $_.ProcessId -Force
}
