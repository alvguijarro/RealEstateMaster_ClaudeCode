@echo off
setlocal enabledelayedexpansion
echo ==========================================
echo   Stopping RealEstateMaster Services...
echo ==========================================
echo.

echo [+] Closing port processes (5000-5005)...
rem Single netstat call for the entire range 5000-5005
for /f "tokens=5" %%a in ('netstat -ano ^| findstr "LISTENING" ^| findstr ":500[0-5]"') do (
    taskkill /F /PID %%a 2>nul
)

echo [+] Cleaning up background Python workers...
rem Use CIM for ultra-fast filtering of RealEstateMaster processes
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name LIKE 'python%%' AND CommandLine LIKE '%%RealEstateMaster%%'\" | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"

echo [+] Cleaning up browser orphans...
rem Mass terminate without /T (tree) to avoid slow recursive scanning
taskkill /F /IM node.exe 2>nul
taskkill /F /IM firefox.exe 2>nul
taskkill /F /IM chrome.exe 2>nul
taskkill /F /IM msedge.exe 2>nul
taskkill /F /IM falkon.exe 2>nul

echo [+] Removing stop flags...
if exist scraper\ENRICH_STOP.flag del scraper\ENRICH_STOP.flag
if exist scraper\BATCH_STOP.flag del scraper\BATCH_STOP.flag
if exist scraper\SCRAPER_STOP.flag del scraper\SCRAPER_STOP.flag

echo.
echo [OK] All services stopped.
timeout /t 2 >nul
