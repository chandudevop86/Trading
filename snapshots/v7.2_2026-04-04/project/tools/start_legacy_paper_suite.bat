@echo off
cd /d F:\Trading
start "Trading Legacy Paper Suite" powershell -NoExit -ExecutionPolicy Bypass -File F:\Trading\tools\start_legacy_paper_suite.ps1 %*
