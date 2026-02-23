@echo off
:: Navigate to the script directory
cd /d "%~dp0"
:: Navigate up to the project root
cd ..
:: Execute the tracker with headless and resume flags
python trends\trends_tracker.py --resume --headless
exit /b %errorlevel%
