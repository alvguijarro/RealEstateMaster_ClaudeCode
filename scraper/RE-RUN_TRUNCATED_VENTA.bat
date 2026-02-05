@echo off
cd /d "%~dp0\.."
echo Starting RE-RUN for Truncated VENTA files (Limit 2000 URLs --> 80000 URLs)...
echo ===============================================================================
echo Target Provinces: A Coruna, Albacete, Alicante, Almeria, Asturias, Badajoz, Baleares, Barcelona, Burgos, Caceres, Cadiz, Castellon, Ciudad Real, Cordoba, Girona, Granada, Guadalajara, Guipuzcoa, Huelva, Jaen, La Rioja, Las Palmas, Leon, Lleida, Lugo, Madrid, Murcia, Navarra, Ourense, Pontevedra, Sevilla, Tarragona, Toledo, Valencia, Valladolid, Vizcaya, Zaragoza
echo ===============================================================================

where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found in PATH.
    pause
    exit /b
)

python scripts/batch_api_scan.py --operation sale --max-pages 2000 --resume --provinces "A Coruna,Albacete,Alicante,Almeria,Asturias,Badajoz,Baleares,Barcelona,Burgos,Caceres,Cadiz,Castellon,Ciudad Real,Cordoba,Girona,Granada,Guadalajara,Guipuzcoa,Huelva,Jaen,La Rioja,Las Palmas,Leon,Lleida,Lugo,Madrid,Murcia,Navarra,Ourense,Pontevedra,Sevilla,Tarragona,Toledo,Valencia,Valladolid,Vizcaya,Zaragoza"

echo ===============================================================================
echo Fix process finished.
pause
