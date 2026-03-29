param(
    [switch]$Once,
    [int]$StreamlitPort = 8501,
    [int]$VinayakPort = 8000
)

$ErrorActionPreference = 'Stop'
Set-Location 'F:\Trading'

$legacyArgs = @('-NoExit', '-Command', "Set-Location 'F:\Trading'; & 'F:\Trading\tools\start_legacy_paper_suite.ps1'" + ($(if ($Once) { ' -Once' } else { '' })))
$uiArgs = @('-NoExit', '-Command', "Set-Location 'F:\Trading'; & 'F:\Trading\tools\start_trading_ui_suite.ps1' -StreamlitPort $StreamlitPort -VinayakPort $VinayakPort")

Start-Process powershell -ArgumentList $legacyArgs
Start-Process powershell -ArgumentList $uiArgs

Write-Host 'Started legacy paper suite launcher.'
Write-Host ('Legacy Streamlit UI target: http://localhost:{0}' -f $StreamlitPort)
Write-Host ('Vinayak API/UI target: http://localhost:{0}' -f $VinayakPort)
