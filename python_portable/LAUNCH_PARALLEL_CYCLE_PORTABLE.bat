@echo off
setlocal
title RealEstateMaster - Ciclo Paralelo de Scraping

REM ── Configurar numero de workers (editar aqui si se desea cambiar) ─────────
set NUM_WORKERS=3

REM ── Configurar PLAYWRIGHT_BROWSERS_PATH ─────────────────────────────────────
if exist "%~dp0browsers" (
    set "PLAYWRIGHT_BROWSERS_PATH=%~dp0browsers"
) else (
    set "PLAYWRIGHT_BROWSERS_PATH=%~dp0..\browsers"
)
echo [+] Playwright Browsers Path: %PLAYWRIGHT_BROWSERS_PATH%

REM ── Exportar API keys y credenciales ────────────────────────────────────────
set "RAPIDAPI_KEY=0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"
set "GOOGLE_API_KEY=AIzaSyB5g2kv8fP4HEnPdhc8megfWQM4TFIp8Oc"
set "TWOCAPTCHA_API_KEY=f49b4e9ed2e2b36add9c6ef3af3e6e4c"
set "CAPSOLVER_API_KEY=CAP-80466E39600EB27CBE3C64207EF3702BEE5F7662B71FCF0323FD4045AA753463"

REM ── Cambiar al directorio raiz del proyecto ──────────────────────────────────
cd /d "%~dp0.."

echo [+] Iniciando ciclo paralelo de scraping con %NUM_WORKERS% workers...
"%~dp0python.exe" "scripts\parallel_worker_launcher.py" --workers %NUM_WORKERS%
pause
