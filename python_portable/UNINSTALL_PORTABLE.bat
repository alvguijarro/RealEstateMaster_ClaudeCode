@echo off
setlocal enabledelayedexpansion
title Real Estate Master - Uninstall Portable Tools

echo ============================================
echo   Real Estate Master - Uninstall Portable Tools
echo ============================================
echo.
echo This script will remove:
echo  1. All installed Python packages (Flask, Pandas, Playwright, etc.)
echo  2. Local Playwright browsers
echo  3. Setup completion markers
echo.
echo It will NOT remove the embedded Python interpreter itself.
echo.
set /p "CONFIRM=Are you sure you want to proceed? (Y/N): "
if /i not "%CONFIRM%"=="Y" exit /b

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
set "PYTHON=%SCRIPT_DIR%python.exe"

if not exist "%PYTHON%" (
    echo ERROR: Portable Python not found. Nothing to uninstall.
    pause
    exit /b
)

echo.
echo [1/3] Uninstalling Python packages...
REM Uninstalling packages one by one to ensure clean removal
"%PYTHON%" -m pip uninstall -y flask flask-socketio pandas openpyxl xlsxwriter numpy scikit-learn google-generativeai playwright playwright-stealth simple-websocket python-socketio python-engineio

echo.
echo [2/3] Removing Playwright browsers...
if exist "browsers" (
    rmdir /s /q "browsers"
    echo Browsers removed.
) else (
    echo Browsers directory not found.
)

echo.
echo [3/3] Removing setup markers...
if exist ".setup_complete" del ".setup_complete"
if exist ".deps_installed" del ".deps_installed"

echo.
echo ============================================
echo   Uninstallation Complete!
echo ============================================
echo.
pause
