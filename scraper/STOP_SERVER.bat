@echo off
echo ============================================
echo   Stopping Idealista Scraper Server...
echo ============================================
echo.
REM Kill any Python processes running on port 5000
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5000 ^| findstr LISTENING') do (
    echo Killing process %%a on port 5000...
    taskkill /F /PID %%a 2>nul
)
REM Also kill any python.exe that might be the server
taskkill /F /IM python.exe 2>nul
echo.
echo Server stopped.

