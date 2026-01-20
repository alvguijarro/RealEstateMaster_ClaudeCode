@echo off
REM Set path to portable browsers so Playwright finds them
set PLAYWRIGHT_BROWSERS_PATH=%~dp0browsers

REM Change directory to the project root (one level up)
cd /d "%~dp0.."

REM Launch main.py using the portable python
"python_portable\python.exe" "main.py"
pause
