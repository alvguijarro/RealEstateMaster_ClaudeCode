@echo off
echo ============================================
echo   Background Enrichment Worker
echo   Enriches API data with Scraper fields
echo ============================================
echo.

cd /d "%~dp0\.."

python scripts\enrich_worker.py --input "scraper\salidas\API_BATCH_*.xlsx" --max-price 300000

echo.
echo Enrichment complete!
pause
