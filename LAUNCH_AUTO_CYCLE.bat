@echo off
TITLE RealEstateMaster - Automated Scraper Cycle
cd /d "%~dp0"
echo Starting Automated Scraper Cycle...
python scripts\automated_cycle.py
pause
