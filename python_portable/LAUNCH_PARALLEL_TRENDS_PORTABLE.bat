@echo off
setlocal
title RealEstateMaster - Trends Paralelo

REM ── Configurar numero de workers (editar aqui si se desea cambiar) ─────────
set NUM_WORKERS=5

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

REM ── Configurar 5 proxies residenciales (Bright Data) ───────────────────────
set "PROXY_1_HOST=brd.superproxy.io"
set "PROXY_1_PORT=33335"
set "PROXY_1_LOGIN=brd-customer-hl_e2c01f5d-zone-residential_proxy1"
set "PROXY_1_PASS=utd291dsjrds"
set "PROXY_2_HOST=brd.superproxy.io"
set "PROXY_2_PORT=33335"
set "PROXY_2_LOGIN=brd-customer-hl_e2c01f5d-zone-residential_proxy2"
set "PROXY_2_PASS=6ege14l7t3rv"
set "PROXY_3_HOST=brd.superproxy.io"
set "PROXY_3_PORT=33335"
set "PROXY_3_LOGIN=brd-customer-hl_e2c01f5d-zone-residential_proxy3"
set "PROXY_3_PASS=kaev4d9gj0rr"
set "PROXY_4_HOST=brd.superproxy.io"
set "PROXY_4_PORT=33335"
set "PROXY_4_LOGIN=brd-customer-hl_e2c01f5d-zone-residential_proxy4"
set "PROXY_4_PASS=k0czpbx9mjke"
set "PROXY_5_HOST=brd.superproxy.io"
set "PROXY_5_PORT=33335"
set "PROXY_5_LOGIN=brd-customer-hl_e2c01f5d-zone-residential_proxy5"
set "PROXY_5_PASS=uu4x0isj6okq"

REM ── Cambiar al directorio raiz del proyecto ──────────────────────────────────
cd /d "%~dp0.."

echo [+] Iniciando trends paralelo con %NUM_WORKERS% workers...
"%~dp0python.exe" "trends\parallel_tracker_launcher.py" --workers %NUM_WORKERS% --resume --headless
pause
