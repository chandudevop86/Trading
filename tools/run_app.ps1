param(
  [int]$Port = 8501
)
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
<<<<<<< HEAD
py -3 -m streamlit run src\Trading.py --server.port $Port --server.address 0.0.0.0
=======
$env:APP_ENV = if ($env:APP_ENV) { $env:APP_ENV } else { 'local' }
py -3 -m streamlit run src\Trading.py --server.port $Port --server.address 127.0.0.1
>>>>>>> feature
