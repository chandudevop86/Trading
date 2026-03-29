param(
    [switch]$Once,
    [int]$StreamlitPort = 8501,
    [int]$VinayakPort = 8000
)

$ErrorActionPreference = 'Stop'
Set-Location 'F:\Trading'

$legacyCommand = "Set-Location 'F:\Trading'; & 'F:\Trading\tools\start_legacy_paper_suite.ps1'"
if ($Once) {
    $legacyCommand += ' -Once'
}

$uiCommand = "Set-Location 'F:\Trading'; & 'F:\Trading\tools\start_trading_ui_suite.ps1' -StreamlitPort $StreamlitPort -VinayakPort $VinayakPort"

Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoExit', '-Command', $legacyCommand)
Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoExit', '-Command', $uiCommand)

Write-Host 'Started legacy paper suite launcher.'
Write-Host ('Legacy Streamlit UI target: http://localhost:{0}' -f $StreamlitPort)
Write-Host ('Vinayak API/UI target: http://localhost:{0}' -f $VinayakPort)
