@echo off
cd /d "%~dp0"
echo ============================================
echo   Restarting RealEstateMaster services...
echo ============================================
echo.

REM Stop all services using the centralized script
call STOP_ALL.bat

echo.
echo Starting Main Menu...
REM Start the Main Menu (Unified Dashboard)
start "RealEstateMaster Main Menu" cmd /c "python main.py"

echo.
echo Services restarted!
timeout /t 3 >nul
exit
