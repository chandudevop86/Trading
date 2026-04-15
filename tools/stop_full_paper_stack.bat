@echo off
set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set REPO_ROOT=%%~fI
cd /d "%REPO_ROOT%"
echo Stopping trading stack windows...
C:\Windows\System32\taskkill.exe /F /FI "WINDOWTITLE eq Trading Legacy Paper Suite*"
C:\Windows\System32\taskkill.exe /F /FI "WINDOWTITLE eq Trading Legacy Streamlit UI*"
C:\Windows\System32\taskkill.exe /F /FI "WINDOWTITLE eq Trading Vinayak API*"
echo.
echo Done. Press any key to close.
pause >nul
