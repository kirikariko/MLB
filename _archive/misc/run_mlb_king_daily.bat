@echo off
REM Daily MLB King collector - 16:30 KST
cd /d "%USERPROFILE%\MLB"
if not exist logs mkdir logs
set PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe
set GIT=C:\Program Files\Git\cmd\git.exe
set GIT_TERMINAL_PROMPT=0
set LOG=logs\mlb_king_%date:~0,4%-%date:~5,2%-%date:~8,2%.log
set PYTHONIOENCODING=utf-8
echo === Start: %date% %time% === >> "%LOG%"
if not exist "%PY%" (
    echo [FATAL] python.exe not found: %PY% >> "%LOG%"
    exit /b 9009
)
REM --- git self-heal: recover from interrupted rebase / unmerged / stale locks ---
REM Remove ALL stale *.lock under .git (any location), abort half-done rebase/merge,
REM then resync local branch to origin/main. Safe: mlb_two.csv owned by cloud dept1,
REM local outputs regenerate each run. ASCII-only comments (cmd reads bat as ANSI).
if exist "%GIT%" (
    echo --- git self-heal --- >> "%LOG%"
    for /r ".git" %%L in (*.lock) do del /f /q "%%L" >nul 2>&1
    "%GIT%" rebase --abort >nul 2>&1
    "%GIT%" merge --abort >nul 2>&1
    "%GIT%" fetch origin main --quiet >> "%LOG%" 2>&1
    "%GIT%" reset --mixed origin/main >> "%LOG%" 2>&1
    "%GIT%" restore --source=origin/main -- mlb_two.csv >> "%LOG%" 2>&1
)
"%PY%" mlb_king.py >> "%LOG%" 2>&1
echo === End: %date% %time% (exit=%ERRORLEVEL%) === >> "%LOG%"
