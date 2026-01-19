@echo off
echo Stopping all Real Estate Master services...

REM Kill all Python processes by port
powershell -Command "5000, 5001, 5002, 5003, 5004 | ForEach-Object { Get-NetTCPConnection -LocalPort $_ -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue } }"

REM Also kill any remaining python.exe processes that might be orphaned
taskkill /F /IM python.exe 2>nul

REM Wait a moment for processes to fully terminate
timeout /t 2 /nobreak >nul

echo All Real Estate Master services have been stopped.
exit
