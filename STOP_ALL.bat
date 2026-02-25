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

REM Kill processes on port 5005 (trends)
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :5005 ^| findstr LISTENING') do (
    echo Killing process %%a on port 5005...
    taskkill /F /PID %%a 2>nul
)

echo.
REM Kill any remaining Python processes related to RealEstateMaster
echo Cleaning up background Python workers...
for /f "tokens=2 delims=," %%a in ('wmic process where "name='python.exe' and (CommandLine like '%%RealEstateMaster%%')" get ProcessId /format:csv 2^>nul ^| findstr /r [0-9]') do (
    taskkill /F /PID %%a 2>nul
)

REM Kill Playwright/Browser orphans
echo Cleaning up browser orphans...
taskkill /F /IM node.exe /T 2>nul
taskkill /F /IM chrome.exe /FI "MODULES eq *playwright*" /T 2>nul
taskkill /F /IM msedge.exe /FI "MODULES eq *playwright*" /T 2>nul
taskkill /F /IM chromium.exe /T 2>nul

REM Delete stop flags to ensure a fresh start
echo Cleaning up stop flags...
if exist scraper\ENRICH_STOP.flag del scraper\ENRICH_STOP.flag
if exist scraper\BATCH_STOP.flag del scraper\BATCH_STOP.flag
if exist scraper\SCRAPER_STOP.flag del scraper\SCRAPER_STOP.flag

echo.
echo All services stopped.
timeout /t 1 >nul
