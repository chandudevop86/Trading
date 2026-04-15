@echo off
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set REPO_ROOT=%%~fI
cd /d "%REPO_ROOT%"
start "Trading UI Suite" powershell -NoExit -ExecutionPolicy Bypass -File "%REPO_ROOT%\tools\start_trading_ui_suite.ps1" %*
