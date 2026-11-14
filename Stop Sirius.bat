@echo off
cd /d "%~dp0"
echo ========================================================
echo Stopping running Sirius Copier instances...
echo ========================================================

powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and ($_.CommandLine -like '*main.py*' -or $_.CommandLine -like '*multiprocessing.spawn*') } | ForEach-Object { Write-Host 'Stopped Sirius Process (PID:' $_.ProcessId ')'; Stop-Process -Id $_.ProcessId -Force }"

if exist "sirius_copier.lock" (
    del /f /q "sirius_copier.lock" 2>nul
)

echo.
echo Sirius Copier has been fully stopped and the lock is cleared.
echo You can now run 'Launch Sirius.bat'.
echo.
pause
