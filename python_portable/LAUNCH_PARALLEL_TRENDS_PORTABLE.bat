@echo off
setlocal
title RealEstateMaster - Trends Paralelo
set NUM_WORKERS=3
cd /d "%~dp0.."
echo [+] Iniciando trends paralelo con %NUM_WORKERS% workers...
"%~dp0python.exe" "trends\parallel_tracker_launcher.py" --workers %NUM_WORKERS% --resume --headless
pause
