param(
    [int]$StreamlitPort = 8501,
    [int]$VinayakPort = 8002
)

$ErrorActionPreference = 'Stop'
Set-Location 'F:\Trading'

$legacyUiCommand = @(
    'Set-Location ''F:\Trading'''
    ('py -3 -m streamlit run src\Trading.py --server.port {0}' -f $StreamlitPort)
) -join '; '

$vinayakApiCommand = @(
    'Set-Location ''F:\Trading'''
    ('py -3 -m uvicorn vinayak.api.main:app --host 0.0.0.0 --port {0}' -f $VinayakPort)
) -join '; '

Start-Process powershell -ArgumentList '-NoExit', '-Command', $legacyUiCommand
Start-Process powershell -ArgumentList '-NoExit', '-Command', $vinayakApiCommand

Write-Host ('Legacy Streamlit UI starting on http://localhost:{0}' -f $StreamlitPort)
Write-Host ('Vinayak API/UI starting on http://localhost:{0}' -f $VinayakPort)
