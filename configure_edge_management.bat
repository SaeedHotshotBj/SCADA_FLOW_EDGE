@echo off
setlocal
cd /d "%~dp0"

if not exist edge_management_config.py (
    echo Missing edge_management_config.py
    pause
    exit /b 1
)

py edge_management_config.py

endlocal
