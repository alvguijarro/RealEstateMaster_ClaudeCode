@echo off
REM Set path to portable browsers
if exist "%~dp0browsers" (
    set PLAYWRIGHT_BROWSERS_PATH=%~dp0browsers
) else (
    set PLAYWRIGHT_BROWSERS_PATH=%~dp0..\browsers
)
set "PLAYWRIGHT_BROWSERS_PATH=%PLAYWRIGHT_BROWSERS_PATH%"
echo [+] Playwright Browsers Path: %PLAYWRIGHT_BROWSERS_PATH%

REM Change directory to project root
cd /d "%~dp0.."

REM Stop all running processes (using root STOP_ALL.bat)
call "STOP_ALL.bat"

REM Wait a bit
timeout /t 2 /nobreak >nul

REM Launch main.py using the portable python
"python_portable\python.exe" "main.py"
pause
