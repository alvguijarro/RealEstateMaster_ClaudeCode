@echo off
echo Stopping Trends Service (Port 5005)...
FOR /F "tokens=5" %%T IN ('netstat -a -n -o ^| findstr :5005') DO (
    echo Killing process %%T
    taskkill /F /PID %%T
)
echo Done.
pause
