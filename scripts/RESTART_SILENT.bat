@echo off
cd /d "%~dp0\.."

REM Restart services WITHOUT opening browser

REM First, stop all services
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5003 ^| findstr LISTENING') do (
    taskkill /F /PID %%a 2>nul
)

for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5001 ^| findstr LISTENING') do (
    taskkill /F /PID %%a 2>nul
)

for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5004 ^| findstr LISTENING') do (
    taskkill /F /PID %%a 2>nul
)

REM Wait for ports to be released
timeout /t 2 >nul

REM Start the scraper server again (no browser)
cd scraper
start "Scraper Server" /min cmd /c "python start.py"
cd ..

timeout /t 2 >nul
