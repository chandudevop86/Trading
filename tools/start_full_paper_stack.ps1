param(
    [switch]$Once,
    [int]$StreamlitPort = 8501,
    [int]$VinayakPort = 8002
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$legacyCommand = "Set-Location '$repoRoot'; & '$repoRoot\tools\start_legacy_paper_suite.ps1'"
if ($Once) {
    $legacyCommand += ' -Once'
}

$uiCommand = "Set-Location '$repoRoot'; & '$repoRoot\tools\start_trading_ui_suite.ps1' -StreamlitPort $StreamlitPort -VinayakPort $VinayakPort"

Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoExit', '-Command', $legacyCommand)
Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoExit', '-Command', $uiCommand)

Write-Host 'Started legacy paper suite launcher.'
Write-Host ('Legacy Streamlit UI target: http://localhost:{0}' -f $StreamlitPort)
Write-Host ('Vinayak API/UI target: http://localhost:{0}' -f $VinayakPort)
