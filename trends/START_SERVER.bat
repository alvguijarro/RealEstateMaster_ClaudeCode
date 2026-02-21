@echo off
cd /d "%~dp0"
echo Starting Trends Application...

REM Activate virtual environment if needed (assuming shared with root or portable python)
IF EXIST "..\python_portable\python.exe" (
    ..\python_portable\python.exe app.py
) ELSE (
    python app.py
)
pause
