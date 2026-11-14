@echo off
setlocal enabledelayedexpansion
title Sirius TSC - Push Changes to GitHub

echo ========================================================
echo        SIRIUS TSC - AUTOMATED GIT PUSH TOOL
echo ========================================================
echo.

cd /d "%~dp0"

REM Step 1: Check git status
echo [*] Checking repository status...
git status -s
echo.

REM Prompt user for commit message
set /p COMMIT_MSG="Enter commit description (leave empty for automatic timestamp): "

if "%COMMIT_MSG%"=="" (
    for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set dt=%%I
    set YYYY=!dt:~0,4!
    set MM=!dt:~4,2!
    set DD=!dt:~6,2!
    set HH=!dt:~8,2!
    set MIN=!dt:~10,2!
    set COMMIT_MSG=Updates !YYYY!-!MM!-!DD! !HH!:!MIN!
)

echo.
echo [*] Staging all tracked & allowed files...
git add .

echo [*] Committing changes with message: "%COMMIT_MSG%"...
git commit -m "%COMMIT_MSG%"

if %errorlevel% neq 0 (
    echo [i] No changes detected to commit or commit cancelled.
    goto finish
)

echo.
echo [*] Pushing to remote repository...
git push origin main

if %errorlevel% equ 0 (
    echo.
    echo [OK] Successfully pushed changes to GitHub!
) else (
    echo.
    echo [X] Push failed. Please check your network connection or GitHub credentials.
)

:finish
echo.
echo ========================================================
pause
