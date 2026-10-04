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

rem Use the current Windows user Run key so no Administrator permission is required.
rem Keep run_edge_management.vbs inside this project folder.
reg add "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v "SCADA Edge Management" /t REG_SZ /d "wscript.exe \"%VBS%"" /f
if errorlevel 1 (
    echo Failed to register automatic startup.
    pause
    exit /b 1
)

rem Remove the older scheduled-task installation when possible.
schtasks /Delete /TN "SCADA FLOW Edge Management" /F >nul 2>&1

start "" wscript.exe "%VBS%"
echo.
echo SCADA FLOW Edge Management Agent installed and started.
echo It will start automatically when this Windows user logs on.
echo Administrator permission is not required for automatic startup.
echo.
pause

endlocal
