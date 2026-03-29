@echo off
cd /d F:\Trading
powershell -ExecutionPolicy Bypass -File F:\Trading\tools\stop_full_paper_stack.ps1 %*
