@echo off
REM Test Runner Script for Windows

echo.
echo ===============================================
echo Running Pytest with Coverage Analysis
echo ===============================================
echo.

REM Navigate to project root
cd /d "%~dp0.."

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run pytest with coverage
pytest tests/ --cov=scripts --cov-report=html --cov-report=term-missing --verbose

REM Check result
if %errorlevel% equ 0 (
    echo.
    echo ===============================================
    echo Tests completed successfully!
    echo Coverage report: htmlcov/index.html
    echo ===============================================
) else (
    echo.
    echo ===============================================
    echo Tests FAILED!
    echo ===============================================
    exit /b 1
)

pause
