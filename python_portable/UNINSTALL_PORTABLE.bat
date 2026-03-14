@echo off
setlocal enabledelayedexpansion
title Real Estate Master - Desinstalar

echo ============================================
echo   Real Estate Master - Desinstalar / Reset
echo ============================================
echo.
echo Que tipo de desinstalacion quieres?
echo.
echo   1. RESET   - Elimina paquetes y marcadores. Puedes volver a ejecutar SETUP.bat
echo   2. LIMPIEZA TOTAL - Elimina TODO lo que SETUP.bat instalo (paquetes + navegadores).
echo                        Deja el PC como estaba antes de ejecutar SETUP.bat.
echo                        Solo quedan: python.exe, DLLs, .bat y LEEME.
echo   3. Cancelar
echo.
set /p "OPCION=Elige opcion (1/2/3): "

if "!OPCION!"=="3" exit /b
if "!OPCION!"=="" exit /b

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"
set "PYTHON=%SCRIPT_DIR%python.exe"

if "!OPCION!"=="2" goto :LIMPIEZA_TOTAL

REM ═════════════════════════════════════════════════════════════════════════════
REM  OPCION 1: RESET (desinstalar paquetes pip, conservar Lib/Scripts/browsers)
REM ═════════════════════════════════════════════════════════════════════════════

echo.
echo [1/3] Desinstalando paquetes Python...
if exist "%PYTHON%" (
    "%PYTHON%" -m pip uninstall -y flask flask-socketio pandas openpyxl xlsxwriter numpy scikit-learn google-generativeai playwright playwright-stealth simple-websocket python-socketio python-engineio httpx google-cloud-bigquery pyarrow joblib requests
    echo Paquetes desinstalados.
) else (
    echo Python no encontrado, saltando desinstalacion.
)

echo.
echo [2/3] Eliminando marcadores de setup...
if exist ".setup_complete" del ".setup_complete"
if exist ".deps_installed" del ".deps_installed"
echo Marcadores eliminados.

echo.
echo [3/3] Reset completado.
echo Para reinstalar todo, ejecuta SETUP.bat.
echo.
pause
exit /b

REM ═════════════════════════════════════════════════════════════════════════════
REM  OPCION 2: LIMPIEZA TOTAL (dejar el PC como antes de SETUP.bat)
REM ═════════════════════════════════════════════════════════════════════════════
:LIMPIEZA_TOTAL

echo.
echo ATENCION: Esto eliminara COMPLETAMENTE:
echo   - Lib\          (paquetes Python instalados)
echo   - Scripts\      (pip y herramientas)
echo   - Include\
echo   - browsers\     (Chromium, Chrome, OperaPortable)
echo   - __pycache__\
echo   - .setup_complete, .deps_installed
echo.
echo Se CONSERVARA:
echo   - python.exe, DLLs y runtime Python
echo   - Todos los .bat (SETUP, START, STOP, etc.)
echo   - LEEME_INSTRUCCIONES.txt
echo.
echo El PC quedara exactamente como antes de ejecutar SETUP.bat.
echo Para volver a usar el programa, solo ejecuta SETUP.bat de nuevo.
echo.

set /p "CONFIRM=Continuar con limpieza total? (S/N): "
if /i not "!CONFIRM!"=="S" (
    echo Cancelado.
    pause
    exit /b
)

echo.
echo [1/4] Eliminando paquetes Python (Lib, Scripts, Include)...
if exist "%SCRIPT_DIR%Lib" (
    echo   Eliminando Lib\...
    rmdir /s /q "%SCRIPT_DIR%Lib" 2>nul
)
if exist "%SCRIPT_DIR%Scripts" (
    echo   Eliminando Scripts\...
    rmdir /s /q "%SCRIPT_DIR%Scripts" 2>nul
)
if exist "%SCRIPT_DIR%Include" (
    echo   Eliminando Include\...
    rmdir /s /q "%SCRIPT_DIR%Include" 2>nul
)
if exist "%SCRIPT_DIR%__pycache__" (
    echo   Eliminando __pycache__\...
    rmdir /s /q "%SCRIPT_DIR%__pycache__" 2>nul
)
echo Paquetes eliminados.

echo.
echo [2/4] Eliminando navegadores...
if exist "%SCRIPT_DIR%browsers" (
    for /d %%D in ("%SCRIPT_DIR%browsers\*") do (
        echo   Eliminando %%~nxD...
        rmdir /s /q "%%D" 2>nul
    )
    for %%F in ("%SCRIPT_DIR%browsers\*") do (
        del "%%F" 2>nul
    )
    echo Navegadores eliminados.
) else (
    echo Carpeta browsers\ no encontrada.
)

echo.
echo [3/4] Eliminando marcadores...
if exist ".setup_complete" del ".setup_complete"
if exist ".deps_installed" del ".deps_installed"
echo Marcadores eliminados.

echo.
echo [4/4] Limpieza total completada.
echo.
echo ============================================
echo   PC limpio. Solo queda el runtime Python.
echo ============================================
echo.
echo Para volver a instalar todo: ejecuta SETUP.bat
echo.
pause
