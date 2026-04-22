@echo off
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Missing .venv\Scripts\python.exe
  echo Create the virtual environment first.
  pause
  exit /b 1
)

echo Installing/updating dependencies...
call ".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
  echo [ERROR] pip install failed.
  pause
  exit /b 1
)

echo Starting CERFA extractor on http://127.0.0.1:8000
start "" http://127.0.0.1:8000
call ".venv\Scripts\python.exe" -m uvicorn app:app --host 0.0.0.0 --port 8000
if errorlevel 1 (
  echo [ERROR] Server failed to start.
  pause
  exit /b 1
)