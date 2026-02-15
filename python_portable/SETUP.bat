@echo off
setlocal enabledelayedexpansion
title Real Estate Master - First Time Setup

echo ============================================
echo   Real Estate Master - Portable Setup
echo ============================================
echo.

REM Get the directory where this script is located
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM Check if python.exe exists, if not copy from ..\scraper\python
if not exist "python.exe" (
    echo [1/4] Setting up portable Python...
    
    if exist "..\scraper\python\python.exe" (
        echo Copying Python from ..\scraper\python to current directory...
        xcopy /E /I /Y "..\scraper\python" "." >nul 2>&1
        if errorlevel 1 (
            echo ERROR: Failed to copy Python. Please check permissions.
            pause
            exit /b 1
        )
        echo Done.
    ) else (
        echo ERROR: Could not find ..\scraper\python\python.exe
        echo Please ensure the workspace is complete.
        pause
        exit /b 1
    )
) else (
    echo [1/4] Portable Python already exists. Skipping copy.
)

echo.
echo [2/4] Installing/updating Python dependencies...

REM Enable pip in the embedded Python
set "PYTHON=%SCRIPT_DIR%python.exe"
set "PIP=%SCRIPT_DIR%Scripts\pip.exe"

REM Check if pip exists
if not exist "%PIP%" (
    echo Installing pip...
    "%PYTHON%" -m ensurepip --upgrade 2>nul
    if errorlevel 1 (
        echo Downloading get-pip.py...
        powershell -Command "Invoke-WebRequest -Uri 'https://bootstrap.pypa.io/get-pip.py' -OutFile 'get-pip.py'" 2>nul
        "%PYTHON%" get-pip.py
        del get-pip.py 2>nul
    )
)

REM Install required packages from master requirements
echo Installing all tool dependencies from requirements_master.txt...
"%PYTHON%" -m pip install --upgrade pip
"%PYTHON%" -m pip install --upgrade -r "..\requirements_master.txt"

echo.
echo [3/4] Installing Playwright browsers (Chromium & Firefox)...
REM Set browser path to shared directory in project root
set PLAYWRIGHT_BROWSERS_PATH=%SCRIPT_DIR%..\browsers
"%PYTHON%" -m playwright install chromium firefox webkit 2>nul
if errorlevel 1 (
    echo WARNING: Playwright browser installation may have issues.
    echo The scraper might not work, but other features will.
)

echo.
echo [4/4] Creating setup completion marker...
echo Setup completed on %date% at %time% > ".setup_complete"

echo.
echo ============================================
echo   Setup Complete!
echo ============================================
echo.
echo You can now run START_PORTABLE.bat to launch the application.
echo.
pause
