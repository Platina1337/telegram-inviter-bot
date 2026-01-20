@echo off
REM Script to add session via CLI
cd /d "%~dp0"
python -m parser.add_session
pause
