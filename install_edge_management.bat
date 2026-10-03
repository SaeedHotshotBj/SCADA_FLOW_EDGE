@echo off
setlocal
cd /d "%~dp0"

if not exist edge_management_config.json (
    echo First run configure_edge_management.bat and enter the pairing code generated in Master.
    pause
    exit /b 1
)

echo Installing required Edge management dependency...
py -m pip install psutil
if errorlevel 1 (
    echo Failed to install psutil.
    pause
    exit /b 1
)

set "VBS=%~dp0run_edge_management.vbs"
schtasks /Create /TN "SCADA FLOW Edge Management" /SC ONLOGON /RL HIGHEST /TR "wscript.exe ""%VBS%""" /F
if errorlevel 1 (
    echo Failed to create the scheduled task.
    pause
    exit /b 1
)

start "" wscript.exe "%VBS%"
echo.
echo SCADA FLOW Edge Management Agent installed and started.
echo It will start automatically when the user logs on.
echo.
pause

endlocal
