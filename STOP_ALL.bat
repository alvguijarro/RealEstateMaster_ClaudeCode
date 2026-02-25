@echo off
setlocal enabledelayedexpansion
echo ==========================================
echo   Stopping RealEstateMaster Services...
echo ==========================================
echo.

REM Define the ports used by the application
set PORTS=5000 5001 5002 5003 5004 5005

echo [+] Closing processes on ports: %PORTS%
for %%p in (%PORTS%) do (
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr :%%p ^| findstr LISTENING 2^>nul') do (
        echo Killing process %%a on port %%p...
        taskkill /F /T /PID %%a 2>nul
    )
)

echo.
echo [+] Cleaning up background Python workers...
REM Using PowerShell for fast and precise process filtering (works on Windows 10/11)
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name = 'python.exe' AND CommandLine LIKE '%%RealEstateMaster%%'\" | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"

echo [+] Cleaning up browser orphans...
taskkill /F /IM node.exe /T 2>nul
taskkill /F /IM chrome.exe /FI "MODULES eq *playwright*" /T 2>nul
taskkill /F /IM msedge.exe /FI "MODULES eq *playwright*" /T 2>nul
taskkill /F /IM chromium.exe /T 2>nul
taskkill /F /IM firefox.exe /T 2>nul

echo.
echo [+] Cleaning up stop flags...
if exist scraper\ENRICH_STOP.flag del scraper\ENRICH_STOP.flag
if exist scraper\BATCH_STOP.flag del scraper\BATCH_STOP.flag
if exist scraper\SCRAPER_STOP.flag del scraper\SCRAPER_STOP.flag

echo.
echo [OK] All services stopped.
timeout /t 2 >nul
