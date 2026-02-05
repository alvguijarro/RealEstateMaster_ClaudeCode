@echo off
cd /d "%~dp0\.."
echo Starting RE-RUN for Truncated ALQUILER files (Limit 2000 URLs --> 80000 URLs)...
echo ===============================================================================
echo Target Provinces: Alicante, Baleares, Barcelona, Cadiz, Granada, Madrid, Malaga, Murcia, Valencia
echo ===============================================================================

where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found in PATH.
    pause
    exit /b
)

python scripts/batch_api_scan.py --operation rent --max-pages 2000 --resume --provinces Alicante,Baleares,Barcelona,Cadiz,Granada,Madrid,Malaga,Murcia,Valencia

echo ===============================================================================
echo Fix process finished.
pause
