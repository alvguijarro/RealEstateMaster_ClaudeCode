@echo off
setlocal enabledelayedexpansion
title Real Estate Master - Portable

REM ── Comprobar que se ha ejecutado SETUP antes ────────────────────────────────
if not exist "%~dp0.setup_complete" (
    echo [!] Setup no detectado o incompleto.
    set /p choice="Ejecutar SETUP.bat ahora? (s/n): "
    if /i "!choice!"=="s" (
        call "%~dp0SETUP.bat"
    ) else (
        echo [!] Iniciando sin verificar dependencias. Pueden producirse errores.
    )
)

REM ── Configurar PLAYWRIGHT_BROWSERS_PATH ─────────────────────────────────────
REM Prioridad: python_portable/browsers (modo portable autocontenido)
if exist "%~dp0browsers" (
    set "PLAYWRIGHT_BROWSERS_PATH=%~dp0browsers"
) else (
    set "PLAYWRIGHT_BROWSERS_PATH=%~dp0..\browsers"
)
echo [+] Playwright Browsers Path: %PLAYWRIGHT_BROWSERS_PATH%

REM ── Exportar API keys y credenciales ────────────────────────────────────────
REM Claves embebidas para funcionamiento sin configuracion manual
set "RAPIDAPI_KEY=0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"
set "GOOGLE_API_KEY=AIzaSyB5g2kv8fP4HEnPdhc8megfWQM4TFIp8Oc"
set "TWOCAPTCHA_API_KEY=f49b4e9ed2e2b36add9c6ef3af3e6e4c"
set "CAPSOLVER_API_KEY=CAP-80466E39600EB27CBE3C64207EF3702BEE5F7662B71FCF0323FD4045AA753463"

REM ── Cambiar al directorio raiz del proyecto ──────────────────────────────────
cd /d "%~dp0.."

REM ── Lanzar el dashboard principal ───────────────────────────────────────────
echo [+] Iniciando RealEstateMaster Dashboard...
echo [i] Si el scraper no abre navegador, revisa los errores en: logs\scraper_server.log
"%~dp0python.exe" "main.py"
pause
