param(
  [int]$Port = 8501
)
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot
py -3 -m streamlit run src\Trading.py --server.port $Port --server.address 0.0.0.0