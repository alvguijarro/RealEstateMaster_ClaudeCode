@echo off
cd /d "%~dp0\.."

REM Stop all services (same as STOP_ALL.bat)

REM Kill processes on port 5000 (Main Menu Dashboard)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5000 ^| findstr LISTENING') do (
    taskkill /F /PID %%a 2>nul
)

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

timeout /t 1 >nul
