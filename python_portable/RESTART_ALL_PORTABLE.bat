@echo off
REM Set path to portable browsers
set PLAYWRIGHT_BROWSERS_PATH=%~dp0browsers

REM Change directory to project root
cd /d "%~dp0.."

REM Stop all running processes (using root STOP_ALL.bat)
call "STOP_ALL.bat"

REM Wait a bit
timeout /t 2 /nobreak >nul

REM Launch main.py using the portable python
"python_portable\python.exe" "main.py"
pause
