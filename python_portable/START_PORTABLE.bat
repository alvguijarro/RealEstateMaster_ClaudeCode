@echo off
setlocal enabledelayedexpansion
title Real Estate Master - Portable

REM Check if setup has been run
if not exist "%~dp0.setup_complete" (
    echo [!] Setup not detected or incomplete.
    set /p choice="Do you want to run SETUP.bat now? (y/n): "
    if /i "!choice!"=="y" (
        call "%~dp0SETUP.bat"
    ) else (
        echo [!] Starting without verification. Errors may occur.
    )
)

REM Detect where the 'browsers' folder is located
REM 1. Check inside python_portable
if exist "%~dp0browsers" (
    set PLAYWRIGHT_BROWSERS_PATH=%~dp0browsers
) else (
    REM 2. Check in project root (standard)
    set PLAYWRIGHT_BROWSERS_PATH=%~dp0..\browsers
)
echo [+] Playwright Browsers Path: %PLAYWRIGHT_BROWSERS_PATH%

REM Change directory to the project root (one level up)
cd /d "%~dp0.."

REM Launch main.py using the portable python
echo [+] Starting Unified Dashboard...
"python_portable\python.exe" "main.py"
pause
