@echo off
echo Stopping Merger Tool server...
taskkill /f /im python.exe /fi "WINDOWTITLE eq *merger*" 2>nul
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":5002"') do taskkill /f /pid %%a 2>nul
echo Server stopped.
