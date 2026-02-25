@echo off
setlocal enabledelayedexpansion
echo ==========================================
echo   Stopping RealEstateMaster Services...
echo ==========================================
echo.

echo [+] Closing port processes (5000-5005)...
for %%p in (5000 5001 5002 5003 5004 5005) do (
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%%p ^| findstr LISTENING 2^>nul') do (
        taskkill /F /T /PID %%a 2>nul
    )
)

echo [+] Cleaning up background Python workers...
powershell -NoProfile -Command "Get-Process python -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like '*RealEstateMaster*' } | Stop-Process -Force -ErrorAction SilentlyContinue"

echo [+] Cleaning up browser orphans...
taskkill /F /IM node.exe /T 2>nul
taskkill /F /IM firefox.exe /T 2>nul
taskkill /F /IM chrome.exe /T 2>nul
taskkill /F /IM msedge.exe /T 2>nul

echo [+] Removing stop flags...
if exist scraper\ENRICH_STOP.flag del scraper\ENRICH_STOP.flag
if exist scraper\BATCH_STOP.flag del scraper\BATCH_STOP.flag
if exist scraper\SCRAPER_STOP.flag del scraper\SCRAPER_STOP.flag

echo.
echo [OK] All services stopped.
timeout /t 2 >nul
