$ErrorActionPreference = 'Stop'

$windowTitles = @(
    'Trading Legacy Paper Suite',
    'Trading Legacy Streamlit UI',
    'Trading Vinayak API'
)

$stopped = $false
foreach ($title in $windowTitles) {
    $output = & taskkill /F /FI "WINDOWTITLE eq $title" 2>&1
    if ($LASTEXITCODE -eq 0) {
        $stopped = $true
        Write-Host "Stopped window title: $title"
    }
    else {
        Write-Host "No running window matched: $title"
    }
}

if (-not $stopped) {
    Write-Host 'No matching trading stack windows found.'
}
