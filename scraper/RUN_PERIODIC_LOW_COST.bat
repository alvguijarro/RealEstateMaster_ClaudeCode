@echo off
cd /d "%~dp0\.."
echo ===============================================================================
echo PERIODIC LOW-COST SCRAPER (< 300,000 EUR)
echo ===============================================================================
echo This script will scan ALL 52 Spanish provinces for properties under 300k.
echo Output files: scraper/salidas/idealista_[Province]_lowcost.xlsx
echo ===============================================================================
echo.
echo IMPORTANT: The Scraper Server (port 5000) must be running!
echo If not running, start it first with RUN_SCRAPER_SERVER.bat
echo.
pause

python scripts/run_periodic_low_cost.py

echo ===============================================================================
echo Periodic scan completed. Check logs in scraper/salidas for details.
pause
