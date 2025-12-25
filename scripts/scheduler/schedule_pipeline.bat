@echo off
REM Pipeline Scheduler for Windows Task Scheduler
REM This script is called by Windows Task Scheduler

setlocal enabledelayedexpansion

REM Get current directory
cd /d "%~dp0"

REM Navigate to project root
cd ..\..

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run orchestrator
python scripts/orchestration/orchestrator.py

REM Check exit code
if %errorlevel% equ 0 (
    echo Pipeline executed successfully
    exit /b 0
) else (
    echo Pipeline failed with error code %errorlevel%
    exit /b 1
)
