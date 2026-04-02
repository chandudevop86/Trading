@echo off
cd /d F:\Trading
start "Trading Legacy Paper Suite" powershell -NoExit -ExecutionPolicy Bypass -File F:\Trading\tools\start_legacy_paper_suite.ps1 %*
start "Trading Legacy Streamlit UI" powershell -NoExit -Command "Set-Location 'F:\Trading'; py -3 -m streamlit run src\Trading.py --server.port 8501"
start "Trading Vinayak API" powershell -NoExit -Command "Set-Location 'F:\Trading'; py -3 -m uvicorn vinayak.api.main:app --host 0.0.0.0 --port 8002"
exit /b 0
