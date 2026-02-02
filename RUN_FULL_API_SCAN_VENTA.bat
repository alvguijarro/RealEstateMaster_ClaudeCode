@echo off
echo Starting Full API Province Scan (VENTA)...
echo =======================================

:: Check if python is available
where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found in PATH.
    pause
    exit /b
)

:: Run the batch scan script with operation sale
python scripts/batch_api_scan.py --operation sale --max-pages 300 --resume

echo =======================================
echo Scan process finished.
pause
