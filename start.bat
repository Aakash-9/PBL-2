@echo off
echo Starting QueryMind...

:: Start backend
start "QueryMind Backend" cmd /k "cd /d %~dp0 && python main.py"

:: Wait 4 seconds for backend to boot
timeout /t 4 /nobreak >nul

:: Start frontend
start "QueryMind Frontend" cmd /k "set PATH=C:\Program Files\nodejs;%PATH% && cd /d %~dp0frontend && npm run dev"

:: Wait 3 seconds for vite to boot
timeout /t 3 /nobreak >nul

:: Open browser
start http://localhost:5173

echo Both servers started. Browser opening at http://localhost:5173
