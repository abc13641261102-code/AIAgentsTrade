@echo off
rem One-click launcher for the TradingAgents Web Console (webconsole/).
rem Double-click this file, or run:  start-console.bat [start.ps1 options]
rem Options are forwarded, e.g.  start-console.bat -Port 8080 -NoBrowser
setlocal
set "ROOT=%~dp0"
set "CONSOLE_DIR=%ROOT%webconsole"

if not exist "%CONSOLE_DIR%\start.ps1" (
    echo [console] webconsole\start.ps1 not found. Is this the TradingAgents repo root?
    pause
    exit /b 1
)

where powershell >nul 2>nul
if errorlevel 1 (
    echo [console] PowerShell not found in PATH.
    pause
    exit /b 1
)

title TradingAgents Web Console
chcp 65001 >nul 2>nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%CONSOLE_DIR%\start.ps1" %*
set "CODE=%ERRORLEVEL%"

echo.
echo [console] Service stopped. Exit code: %CODE%
pause
exit /b %CODE%
