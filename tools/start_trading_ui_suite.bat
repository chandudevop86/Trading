@echo off
cd /d F:\Trading
start "Trading UI Suite" powershell -NoExit -ExecutionPolicy Bypass -File F:\Trading\tools\start_trading_ui_suite.ps1 %*
