@echo off
title Understand Dashboard - messaging_system_websocket

set "DASH_DIR=C:\Users\kirill\.claude\plugins\cache\understand-anything\understand-anything\2.8.1\packages\dashboard"
set "GRAPH_DIR=C:\Users\kirill\Documents\vscode\FigZnaet\Exams\JS\src\messaging_system_websocket"

if not exist "%DASH_DIR%" (
  echo [ERROR] Dashboard folder not found:
  echo   %DASH_DIR%
  echo Plugin version may have changed - fix DASH_DIR in this file.
  pause
  exit /b 1
)

if not exist "%GRAPH_DIR%\.understand-anything\knowledge-graph.json" (
  echo [ERROR] Knowledge graph not found:
  echo   %GRAPH_DIR%\.understand-anything\knowledge-graph.json
  echo Run /understand in Claude Code first.
  pause
  exit /b 1
)

echo Starting dashboard...
echo Copy the "Dashboard URL: http://127.0.0.1:.../?token=..." line below
echo and open it in your browser. Press Ctrl+C to stop.
echo.

cd /d "%DASH_DIR%"
call npx vite --host 127.0.0.1

echo.
echo Server stopped.
pause
