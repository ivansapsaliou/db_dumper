@echo off
title DB Dump Manager
echo.
echo  ██████╗ ██████╗     ██████╗ ██╗   ██╗███╗   ███╗██████╗ 
echo  ██╔══██╗██╔══██╗    ██╔══██╗██║   ██║████╗ ████║██╔══██╗
echo  ██║  ██║██████╔╝    ██║  ██║██║   ██║██╔████╔██║██████╔╝
echo  ██║  ██║██╔══██╗    ██║  ██║██║   ██║██║╚██╔╝██║██╔═══╝ 
echo  ██████╔╝██████╔╝    ██████╔╝╚██████╔╝██║ ╚═╝ ██║██║     
echo  ╚═════╝ ╚═════╝     ╚═════╝  ╚═════╝ ╚═╝     ╚═╝╚═╝     
echo.
echo  Manager v1.0 — No DB client required
echo  ─────────────────────────────────────────────────────────
echo.

:: Check Python
python --version > nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Please install Python 3.10+ from python.org
    pause
    exit /b 1
)

:: Install deps if needed
if not exist ".venv" (
    echo [SETUP] Creating virtual environment...
    python -m venv .venv
    echo [SETUP] Installing dependencies...
    .venv\Scripts\pip install -r requirements.txt --quiet
    echo [SETUP] Done!
    echo.
)

:: Activate venv and run
echo [START] Launching DB Dump Manager...
echo [INFO]  Open browser: http://localhost:5000
echo.
start http://localhost:5000
.venv\Scripts\python app.py

pause
