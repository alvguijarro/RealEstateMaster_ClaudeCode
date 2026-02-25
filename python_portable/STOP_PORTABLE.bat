@echo off
setlocal
title Real Estate Master - Stop Services

REM Change directory to project root (one level up)
cd /d "%~dp0.."

if exist "STOP_ALL.bat" (
    echo [+] Llamando al script de parada general...
    call "STOP_ALL.bat"
) else (
    echo [!] No se encuentra STOP_ALL.bat en la raiz del proyecto.
    pause
)
