@echo off
cd /d "%~dp0\.."

REM Start the scraper server WITHOUT opening browser
cd scraper
start "Scraper Server" /min cmd /c "python start.py"
cd ..

timeout /t 2 >nul
