@echo off
cd /d "%~dp0"
echo ============================================
echo   Restarting RealEstateMaster services...
echo ============================================
echo.

REM First, stop all services
echo Stopping services...

REM Kill processes on port 5003 (scraper)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5003 ^| findstr LISTENING') do (
    taskkill /F /PID %%a 2>nul
)

REM Kill processes on port 5001 (analyzer)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5001 ^| findstr LISTENING') do (
    taskkill /F /PID %%a 2>nul
)

REM Kill processes on port 5004 (metrics)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5004 ^| findstr LISTENING') do (
    taskkill /F /PID %%a 2>nul
)

echo Services stopped.
echo.

REM Wait a moment for ports to be released
timeout /t 2 >nul

REM Start the scraper server again
echo Starting scraper server...
cd scraper
start "Scraper Server" /min cmd /c "python start.py"
cd ..

echo.
echo Services restarted!
timeout /t 2 >nul
