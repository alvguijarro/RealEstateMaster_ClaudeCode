@echo off
cd /d "%~dp0"
echo Starting RealEstateMaster services...
echo.

REM Start the scraper server
cd scraper
start "Scraper Server" /min cmd /c "python start.py"
cd ..

REM Wait for server to start, then open browser
echo Waiting for server to start...
timeout /t 3 >nul
start "" "http://127.0.0.1:5003"

echo Server started and browser opened!

