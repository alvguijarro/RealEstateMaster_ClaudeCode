@echo off
cd /d "%~dp0\.."
echo Starting Full API Province Scan...
echo ================================

:: Check if python is available
where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found in PATH.
    pause
    exit /b
)

:: Run the batch scan script
python scripts/batch_api_scan.py --operation rent --max-pages 2000 --resume

echo ================================
echo Scan process finished.
pause
