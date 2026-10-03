@echo off
setlocal

schtasks /Delete /TN "SCADA FLOW Edge Management" /F

echo SCADA FLOW Edge Management scheduled task removed.
pause

endlocal
