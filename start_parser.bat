@echo off
REM Start Parser Service

echo Starting Inviter Parser Service...

REM Change to project root directory
cd /d "%~dp0"

REM Load environment variables
if exist ".env" (
    for /f "delims== tokens=1,2" %%a in ('type .env ^| findstr /v "^#"') do (
        set %%a=%%b
    )
)

python -m uvicorn parser.main:app --host 0.0.0.0 --port 8001 --reload

pause
