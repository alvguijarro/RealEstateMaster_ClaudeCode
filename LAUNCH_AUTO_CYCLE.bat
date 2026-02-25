@echo off
TITLE RealEstateMaster - Automated Scraper Cycle
cd /d "%~dp0"
echo Starting Automated Scraper Cycle...
"%~dp0python_portable\python.exe" scripts\automated_cycle.py
pause
