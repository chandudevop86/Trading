@echo off
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set REPO_ROOT=%%~fI
cd /d "%REPO_ROOT%"
start "Trading Legacy Paper Suite" powershell -NoExit -ExecutionPolicy Bypass -File "%REPO_ROOT%\tools\start_legacy_paper_suite.ps1" %*
start "Trading Legacy Streamlit UI" powershell -NoExit -Command "Set-Location '%REPO_ROOT%'; py -3 -m streamlit run src\Trading.py --server.port 8501"
start "Trading Vinayak API" powershell -NoExit -Command "Set-Location '%REPO_ROOT%'; py -3 -m uvicorn app.main:app --host 0.0.0.0 --port 8002"
exit /b 0

