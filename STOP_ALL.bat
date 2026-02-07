@echo off
echo Stopping all RealEstateMaster services...
echo.

REM Kill processes on port 5000 (Main Menu Dashboard)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5000 ^| findstr LISTENING') do (
    echo Killing process %%a on port 5000...
    taskkill /F /PID %%a 2>nul
)

REM Kill processes on port 5003 (scraper)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5003 ^| findstr LISTENING') do (
    echo Killing process %%a on port 5003...
    taskkill /F /PID %%a 2>nul
)

REM Kill processes on port 5001 (analyzer)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5001 ^| findstr LISTENING') do (
    echo Killing process %%a on port 5001...
    taskkill /F /PID %%a 2>nul
)

REM Kill processes on port 5004 (metrics)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5004 ^| findstr LISTENING') do (
    echo Killing process %%a on port 5004...
    taskkill /F /PID %%a 2>nul
)

REM Kill processes on port 5002 (merger)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5002 ^| findstr LISTENING') do (
    echo Killing process %%a on port 5002...
    taskkill /F /PID %%a 2>nul
)

echo.
echo All services stopped.
timeout /t 1 >nul
