@echo off
cd /d "%~dp0"
echo Starting Idealista Scraper Server...
echo.
echo Server will run in a minimized window.
echo Open http://127.0.0.1:5000 in your browser.
echo.
start "Idealista Server" /min cmd /c "python start.py"
echo Server started! This window will close...
timeout /t 2 >nul
