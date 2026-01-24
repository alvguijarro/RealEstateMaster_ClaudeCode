@echo off
setlocal enabledelayedexpansion
title Real Estate Master - Uninstall Portable Tools

echo ============================================
echo   Real Estate Master - Uninstall Portable Tools
echo ============================================
echo.
echo This script will perform a FACTORY RESET of the portable environment.
echo.
echo It will remove:
echo  1. All installed setup markers (.setup_complete)
echo  2. The local 'browsers' folder (Playwright)
echo.

REM Check if we are in a position to do a Full Factory Reset (Source exists)
set "CAN_FACTORY_RESET=0"
if exist "..\scraper\python\python.exe" (
    set "CAN_FACTORY_RESET=1"
    echo  3. The ENTIRE embedded Python environment (python.exe, Lib, Scripts)
    echo     (Because a backup was found in ..\scraper\python)
) else (
    echo  3. Installed Python libraries (flask, pandas, etc.)
    echo     (SAFE MODE: python.exe is kept because no backup source was found)
)

echo.
set /p "CONFIRM=Are you sure you want to proceed? (Y/N): "
if /i not "%CONFIRM%"=="Y" exit /b

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
set "PYTHON=%SCRIPT_DIR%python.exe"

echo.
if "%CAN_FACTORY_RESET%"=="1" (
    echo [1/3] Performing Factory Reset (Deleting Python)...
    
    if exist "python.exe" del /f /q "python.exe" 2>nul
    if exist "pythonw.exe" del /f /q "pythonw.exe" 2>nul
    if exist "python3.dll" del /f /q "python3.dll" 2>nul
    if exist "python312.dll" del /f /q "python312.dll" 2>nul
    
    REM Remove directories
    if exist "Lib" rmdir /s /q "Lib" 2>nul
    if exist "Scripts" rmdir /s /q "Scripts" 2>nul
    if exist "Include" rmdir /s /q "Include" 2>nul
    if exist "tcl" rmdir /s /q "tcl" 2>nul
    if exist "__pycache__" rmdir /s /q "__pycache__" 2>nul
    
    REM Clean up remaining DLLs and PYDs
    del /q *.dll 2>nul
    del /q *.pyd 2>nul
    del /q *.cat 2>nul
    del /q *.zip 2>nul
    del /q ._pth 2>nul
    
    echo Python environment removed.
) else (
    echo [1/3] Safe Mode: Uninstalling packages...
    if exist "%PYTHON%" (
        "%PYTHON%" -m pip uninstall -y flask flask-socketio pandas openpyxl xlsxwriter numpy scikit-learn google-generativeai playwright playwright-stealth simple-websocket python-socketio python-engineio
    ) else (
        echo Python not found, skipping package uninstall.
    )
)

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
