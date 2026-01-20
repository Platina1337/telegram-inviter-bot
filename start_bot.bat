@echo off
REM Start Bot

echo Starting Inviter Bot...

REM Change to project root directory
cd /d "%~dp0"

REM Load environment variables
if exist ".env" (
    for /f "delims== tokens=1,2" %%a in ('type .env ^| findstr /v "^#"') do (
        set %%a=%%b
    )
)

python -m bot.bot_main

pause
