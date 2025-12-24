@echo off
setlocal enabledelayedexpansion

echo.
echo ================================================
echo E-Commerce Data Pipeline - Setup Script
echo ================================================
echo.

REM Check Python Installation
echo [1/5] Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo Python is not installed or not in PATH.
    echo Please install Python 3.9 or higher.
    pause
    exit /b 1
)
for /f "tokens=*" %%i in ('python --version') do set PYTHON_VERSION=%%i
echo ✓ Found: %PYTHON_VERSION%
echo.

REM Create Virtual Environment
echo [2/5] Creating Python virtual environment...
if exist "venv" (
    echo Virtual environment already exists.
) else (
    python -m venv venv
    echo ✓ Virtual environment created
)
echo.

REM Activate Virtual Environment
echo [3/5] Activating virtual environment...
call venv\Scripts\activate.bat
echo ✓ Virtual environment activated
echo.

REM Install Dependencies
echo [4/5] Installing Python dependencies...
python -m pip install --upgrade pip
pip install -r requirements.txt
echo ✓ Dependencies installed
echo.

REM Setup Environment Variables
echo [5/5] Setting up environment variables...
if not exist ".env" (
    copy .env.example .env
    echo ✓ .env file created from .env.example
    echo Note: Update .env with your database credentials
) else (
    echo ✓ .env file already exists
)

echo.
echo ================================================
echo Setup Complete!
echo ================================================
echo.
pause
