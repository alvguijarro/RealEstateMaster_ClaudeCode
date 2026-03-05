@echo off
cd /d "%~dp0"
echo Starting RealEstateMaster Main Menu...
echo.

REM Start the Main Menu (Unified Dashboard)
REM This script handles the rest (scraper, analyzer, browser)
echo [i] Si el scraper no abre navegador, revisa los errores en: logs\scraper_server.log
"%~dp0python_portable\python.exe" main.py


