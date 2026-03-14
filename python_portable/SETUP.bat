@echo off
setlocal enabledelayedexpansion
title Real Estate Master - First Time Setup

echo ============================================
echo   Real Estate Master - Portable Setup
echo ============================================
echo.

REM Get the directory where this script is located (python_portable/)
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

set "PYTHON=%SCRIPT_DIR%python.exe"
set "PIP=%SCRIPT_DIR%Scripts\pip.exe"

REM ── Paso 1/6: Verificar Python portable ────────────────────────────────────
echo [1/6] Verificando Python portable...
if not exist "%PYTHON%" (
    echo ERROR: No se encuentra python.exe en %SCRIPT_DIR%
    echo Asegurate de copiar la carpeta RealEstateMaster completa, incluyendo python_portable.
    pause
    exit /b 1
)
"%PYTHON%" --version
echo OK.

REM ── Paso 2/6: Bootstrapping pip ────────────────────────────────────────────
echo.
echo [2/6] Actualizando pip e instalando dependencias...

if not exist "%PIP%" (
    echo Bootstrapping pip...
    "%PYTHON%" -m ensurepip --upgrade 2>nul
    if errorlevel 1 (
        echo ensurepip no disponible, descargando get-pip.py...
        REM Intentar con curl.exe (incluido en Windows 10+)
        curl.exe -sS -o "%SCRIPT_DIR%get-pip.py" "https://bootstrap.pypa.io/get-pip.py" 2>nul
        if not exist "%SCRIPT_DIR%get-pip.py" (
            REM Fallback a PowerShell si curl no funciona
            powershell -Command "Invoke-WebRequest -Uri 'https://bootstrap.pypa.io/get-pip.py' -OutFile '%SCRIPT_DIR%get-pip.py'"
        )
        "%PYTHON%" "%SCRIPT_DIR%get-pip.py"
        del "%SCRIPT_DIR%get-pip.py" 2>nul
    )
)

REM Actualizar pip
"%PYTHON%" -m pip install --upgrade pip --quiet

REM ── Paso 3/6: Instalar paquetes desde requirements_master.txt ──────────────
echo.
echo [3/6] Instalando paquetes Python...

set "REQS=%SCRIPT_DIR%..\requirements_master.txt"
if exist "%REQS%" (
    echo Instalando paquetes desde requirements_master.txt...
    "%PYTHON%" -m pip install --upgrade -r "%REQS%"
) else (
    echo WARN: requirements_master.txt no encontrado. Instalando paquetes base...
    "%PYTHON%" -m pip install flask flask-socketio pandas openpyxl xlsxwriter numpy scikit-learn google-generativeai playwright playwright-stealth requests python-socketio joblib google-cloud-bigquery pyarrow httpx
)

REM Instalar paquetes clave de forma explicita para garantizar disponibilidad
echo Verificando paquetes clave adicionales...
"%PYTHON%" -m pip install --upgrade google-cloud-bigquery pyarrow httpx --quiet

REM ── Paso 4/6: Instalar navegadores Playwright ─────────────────────────────
echo.
echo [4/6] Instalando navegadores Playwright...

REM CRITICO: PLAYWRIGHT_BROWSERS_PATH debe apuntar a python_portable/browsers
REM ANTES de ejecutar playwright install para que los navegadores queden en la carpeta portable
set "PLAYWRIGHT_BROWSERS_PATH=%SCRIPT_DIR%browsers"
if not exist "%PLAYWRIGHT_BROWSERS_PATH%" mkdir "%PLAYWRIGHT_BROWSERS_PATH%"
echo Playwright Browsers Path: %PLAYWRIGHT_BROWSERS_PATH%

REM Instalar solo los navegadores que realmente se usan:
REM   - chromium: motor principal del scraper (incluye headless shell)
REM   - chrome: Google Chrome para rotacion en el pool de navegadores
REM NO se instalan webkit ni firefox (no se usan en produccion)
echo Instalando Chromium (motor principal)...
"%PYTHON%" -m playwright install chromium
if errorlevel 1 (
    echo ERROR: No se pudo instalar Chromium. El scraper necesita este navegador.
    echo Verifica tu conexion a internet e intentalo de nuevo.
    pause
    exit /b 1
)

echo Instalando Google Chrome (rotacion de navegadores)...
"%PYTHON%" -m playwright install chrome
if errorlevel 1 (
    echo WARN: No se pudo instalar Chrome. El scraper funcionara solo con Chromium.
)

REM ── Paso 5/6: Descargar OperaPortable (opcional) ──────────────────────────
echo.
echo [5/6] Configurando OperaPortable (opcional, para worker Phase 3)...

set "OPERA_DIR=%SCRIPT_DIR%browsers\OperaPortable"
set "OPERA_EXE=%OPERA_DIR%\App\Opera\opera.exe"

if exist "%OPERA_EXE%" (
    echo OperaPortable ya esta instalado. Saltando.
) else (
    echo OperaPortable no encontrado. Intentando descarga automatica...
    set "OPERA_PAF=OperaPortable_127.0.5778.14.paf.exe"
    set "OPERA_URL=https://sourceforge.net/projects/portableapps/files/Opera%%20Portable/!OPERA_PAF!/download"
    set "OPERA_DOWNLOAD=%SCRIPT_DIR%!OPERA_PAF!"

    REM Descargar con curl.exe (Windows 10+), seguir redirecciones
    echo Descargando !OPERA_PAF! desde SourceForge...
    curl.exe -L -o "!OPERA_DOWNLOAD!" "!OPERA_URL!" 2>nul
    if not exist "!OPERA_DOWNLOAD!" (
        REM Fallback a PowerShell
        echo curl no disponible, usando PowerShell...
        powershell -Command "Invoke-WebRequest -Uri '!OPERA_URL!' -OutFile '!OPERA_DOWNLOAD!' -MaximumRedirection 10" 2>nul
    )

    if exist "!OPERA_DOWNLOAD!" (
        REM Intentar extraccion silenciosa con 7z
        where 7z >nul 2>nul
        if !errorlevel! equ 0 (
            echo Extrayendo con 7z...
            if not exist "%OPERA_DIR%" mkdir "%OPERA_DIR%"
            7z x -o"%OPERA_DIR%" "!OPERA_DOWNLOAD!" -y >nul 2>nul
            if exist "%OPERA_EXE%" (
                echo OperaPortable instalado correctamente.
            ) else (
                echo WARN: Extraccion con 7z no produjo opera.exe.
                echo Ejecutando instalador PAF interactivamente...
                echo Selecciona la carpeta: %OPERA_DIR%
                "!OPERA_DOWNLOAD!"
            )
        ) else (
            REM Sin 7z: ejecutar el instalador PAF interactivamente
            echo 7z no disponible. Ejecutando instalador interactivo...
            echo IMPORTANTE: Selecciona como destino: %OPERA_DIR%
            "!OPERA_DOWNLOAD!"
        )
        REM Limpiar el instalador descargado
        del "!OPERA_DOWNLOAD!" 2>nul
    ) else (
        echo WARN: No se pudo descargar OperaPortable.
        echo El scraper funcionara sin Opera ^(worker Phase 3 se omitira^).
    )
)

REM ── Paso 6/6: Limpieza de navegadores obsoletos ───────────────────────────
echo.
echo [6/6] Limpiando navegadores obsoletos...

set "BROWSERS_DIR=%SCRIPT_DIR%browsers"
set "CLEANED=0"

REM Firefox (no se usa: "Juggler hang issues")
for /d %%D in ("%BROWSERS_DIR%\firefox-*") do (
    echo Eliminando %%~nxD...
    rmdir /s /q "%%D" 2>nul
    set "CLEANED=1"
)

REM WebKit (config lo lista pero codigo lanza chromium en su lugar)
for /d %%D in ("%BROWSERS_DIR%\webkit-*") do (
    echo Eliminando %%~nxD...
    rmdir /s /q "%%D" 2>nul
    set "CLEANED=1"
)

REM FalkonPortable (blacklisted: stability issues)
if exist "%BROWSERS_DIR%\FalkonPortable" (
    echo Eliminando FalkonPortable...
    rmdir /s /q "%BROWSERS_DIR%\FalkonPortable" 2>nul
    set "CLEANED=1"
)

REM IronPortable (no esta en ningun pool activo)
if exist "%BROWSERS_DIR%\IronPortable" (
    echo Eliminando IronPortable...
    rmdir /s /q "%BROWSERS_DIR%\IronPortable" 2>nul
    set "CLEANED=1"
)

REM GoogleChromePortable (codigo muerto: get_browser_executable_path retorna None)
if exist "%BROWSERS_DIR%\GoogleChromePortable" (
    echo Eliminando GoogleChromePortable...
    rmdir /s /q "%BROWSERS_DIR%\GoogleChromePortable" 2>nul
    set "CLEANED=1"
)

REM LibreWolfPortable (no se usa)
for /d %%D in ("%BROWSERS_DIR%\LibreWolfPortable*") do (
    echo Eliminando %%~nxD...
    rmdir /s /q "%%D" 2>nul
    set "CLEANED=1"
)

REM winldd (herramienta Linux, no necesaria en Windows)
for /d %%D in ("%BROWSERS_DIR%\winldd-*") do (
    echo Eliminando %%~nxD...
    rmdir /s /q "%%D" 2>nul
    set "CLEANED=1"
)

REM Instaladores PAF sobrantes (.paf.exe)
for %%F in ("%BROWSERS_DIR%\*.paf.exe") do (
    echo Eliminando %%~nxF...
    del "%%F" 2>nul
    set "CLEANED=1"
)

if "!CLEANED!"=="0" (
    echo No se encontraron navegadores obsoletos.
) else (
    echo Limpieza completada.
)

REM ── Marcar setup como completado ───────────────────────────────────────────
echo.
echo Marcando setup como completado...
echo Setup completado el %date% a las %time% > "%SCRIPT_DIR%.setup_complete"

echo.
echo ============================================
echo   Setup completado correctamente!
echo ============================================
echo.
echo Navegadores instalados:
echo   - Chromium (motor principal + headless shell)
echo   - Google Chrome (rotacion de navegadores)
if exist "%OPERA_EXE%" echo   - OperaPortable (worker Phase 3)
echo.
echo Ahora puedes ejecutar:
echo   START_PORTABLE.bat            - Iniciar el dashboard principal
echo   LAUNCH_AUTO_CYCLE_PORTABLE.bat - Iniciar el ciclo de scraping automatico
echo.
pause
