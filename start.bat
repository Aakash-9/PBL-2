@echo off
echo Starting QueryMind...

:: Start backend
start "QueryMind Backend" cmd /k "cd /d %~dp0 && python main.py"

:: Wait for backend to boot
timeout /t 5 /nobreak >nul

:: Start Next.js frontend
start "QueryMind Frontend" cmd /k "set PATH=C:\Program Files\nodejs;%PATH% && cd /d %~dp0querymind-master && npm run dev"

:: Wait for Next.js to boot
timeout /t 5 /nobreak >nul

:: Open browser
start http://localhost:3000

echo.
echo ✓ Backend  running at http://localhost:8000
echo ✓ Frontend running at http://localhost:3000
echo.
