@echo off
powershell -Command "5000, 5001, 5002, 5003, 5004 | ForEach-Object { Get-NetTCPConnection -LocalPort $_ -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue } }"
echo All Real Estate Master services have been stopped.
