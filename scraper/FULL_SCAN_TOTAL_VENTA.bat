@echo off
cd /d "%~dp0\.."
echo Starting COMPLETE Scan of ALL Spanish Provinces (VENTA) - 2000 Pages...
echo ===============================================================================
echo Target: All 52 Provinces (Alava to Melilla)
echo Configuration: 2000 Pages limit, API Pagination Bypass active.
echo ===============================================================================

where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found in PATH.
    pause
    exit /b
)

python scripts/batch_api_scan.py --operation sale --max-pages 2000 --resume

echo ===============================================================================
echo Complete Scan finished.
pause
