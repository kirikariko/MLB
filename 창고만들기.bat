@echo off
REM ============================================================
REM  MLB data warehouse builder (StatsAPI 2015..this year)
REM  ASCII-only on purpose: cmd parses .bat as cp949 before chcp
REM  takes effect, so Korean text here breaks parsing (2026-09-04).
REM  Korean messages are printed by python instead.
REM ============================================================
setlocal EnableDelayedExpansion
cd /d "%~dp0"
title MLB warehouse build
set PYTHONIOENCODING=utf-8
chcp 65001 >nul

if not exist "logs" mkdir "logs"
set "LOG=logs\warehouse_build.log"
for /f %%i in ('python -c "import datetime;print(datetime.date.today().year)"') do set "YEAR=%%i"
echo ===== RUN %DATE% %TIME% ===== >> "%LOG%"

python scripts\warehouse_build.py --banner

echo [1/3] smoke test (this year, 20 games)...
python scripts\warehouse_build.py --seasons !YEAR! --limit 20 --workers 4 >> "%LOG%" 2>&1
if errorlevel 1 (
  echo   FAILED. last lines of logs\warehouse_build.log:
  powershell -NoProfile -Command "Get-Content '%LOG%' -Tail 15"
  goto :END
)
python scripts\warehouse_build.py --check 20
if errorlevel 1 (
  echo   FAILED: parsed output is empty. show the log to Claude.
  goto :END
)
echo   OK.
echo.

echo [2/3] full build 2015-!YEAR! (1-3 hours; progress in logs\warehouse_build.log)
python scripts\warehouse_build.py --seasons 2015-!YEAR! --workers 6 >> "%LOG%" 2>&1
if errorlevel 1 (
  echo   FAILED. run again to resume. last lines:
  powershell -NoProfile -Command "Get-Content '%LOG%' -Tail 15"
  goto :END
)
python scripts\warehouse_build.py --summary
echo.

echo [3/3] git push...
if exist ".git\index.lock" del /f /q ".git\index.lock"
if exist ".git\HEAD.lock" del /f /q ".git\HEAD.lock"
git add data\warehouse scripts\warehouse_build.py *.bat >> "%LOG%" 2>&1
git -c core.quotepath=false commit -F scripts\warehouse_commit_msg.txt >> "%LOG%" 2>&1
git push >> "%LOG%" 2>&1
if errorlevel 1 (
  echo   push failed. files are in data\warehouse locally. tell Claude.
) else (
  echo   pushed.
)
echo.
echo ===== DONE. tell Claude: "warehouse uploaded" =====

:END
echo.
echo press any key to close.
pause >nul
endlocal
