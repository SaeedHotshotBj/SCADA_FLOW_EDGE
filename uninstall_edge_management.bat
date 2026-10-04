@echo off
setlocal

reg delete "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v "SCADA Edge Management" /f >nul 2>&1
schtasks /Delete /TN "SCADA FLOW Edge Management" /F >nul 2>&1

echo SCADA FLOW Edge Management automatic startup removed.
pause

endlocal
