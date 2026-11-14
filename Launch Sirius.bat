@echo off
cd /d "%~dp0"
echo Starting Sirius Telegram Signal Copier...
:run_copier
python main.py

if %errorlevel% neq 0 (
    echo [X] App crashed! Alerting and restarting in 5 seconds...
    
    REM Send Telegram alert
    python alert.py "Sirius Copier" "crash"
    
    REM Play a sound alert
    start /b powershell -c "(New-Object Media.SoundPlayer 'C:\Windows\Media\Windows Background.wav').PlaySync()"
    
    REM Show a Windows popup alert
    start /b powershell -c "Add-Type -AssemblyName PresentationFramework; [System.Windows.MessageBox]::Show('The Sirius Copier has crashed and is restarting.', 'Sirius Alert', 'OK', 'Warning')"

    timeout /t 5
    
    REM Send "Back up" Telegram alert
    python alert.py "Sirius Copier" "recovery"
    
    goto run_copier
)

pause
