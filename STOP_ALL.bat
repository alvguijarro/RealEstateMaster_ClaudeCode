@echo off
setlocal
echo ==========================================
echo   Stopping RealEstateMaster Services...
echo ==========================================
echo.

echo [+] Killing processes and cleaning workers...
REM Using a single PowerShell call for everything (much faster and avoids CMD parsing loops)
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command ^
    "$ports = 5000, 5001, 5002, 5003, 5004, 5005; " ^
    "foreach ($p in $ports) { " ^
    "  $conn = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue; " ^
    "  foreach ($c in $conn) { " ^
    "    Write-Host \"Cleaning port $p (PID $($c.OwningProcess))...\"; " ^
    "    Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue; " ^
    "  } " ^
    "}; " ^
    "Get-CimInstance Win32_Process -Filter \"Name = 'python.exe' AND CommandLine LIKE '%%RealEstateMaster%%'\" | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"

echo.
echo [+] Cleaning up browser orphans...
taskkill /F /IM node.exe /T 2>nul
taskkill /F /IM firefox.exe /T 2>nul
taskkill /F /IM chrome.exe /FI "MODULES eq *playwright*" /T 2>nul
taskkill /F /IM msedge.exe /FI "MODULES eq *playwright*" /T 2>nul
taskkill /F /IM chromium.exe /T 2>nul

echo [+] Removing stop flags...
if exist scraper\ENRICH_STOP.flag del scraper\ENRICH_STOP.flag
if exist scraper\BATCH_STOP.flag del scraper\BATCH_STOP.flag
if exist scraper\SCRAPER_STOP.flag del scraper\SCRAPER_STOP.flag

echo.
echo [OK] All services stopped.
timeout /t 2 >nul
