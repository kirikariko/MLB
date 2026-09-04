@echo off
REM ============================================================
REM  run_mlb_king.bat - MLB_KING.csv daily collector (Task Scheduler)
REM  Runs daily 16:30 KST (1h before cloud dept1 at 17:30).
REM  Must run on owner's PC (mlb_king.py hits MLB StatsAPI + FanGraphs;
REM  Claude sandbox blocks those APIs).
REM  Outputs: MLB_KING.csv / mlb_king.json / dept3-1.json / dept3-2.json
REM           + GitHub push + Drive upload (inside mlb_king.py)
REM  Incident history: 2026-08-05, 08-16 missed manual run -> dept3 HALT.
REM  ASCII-only: cmd reads .bat as ANSI/cp949; Korean here would mojibake
REM  and its parens can break if() blocks (2026-08-20 incident).
REM ============================================================
setlocal EnableDelayedExpansion
cd /d "%~dp0.."
if not exist "logs" mkdir "logs"
set "LOG=logs\mlb_king.log"
set "PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
set "GIT=C:\Program Files\Git\cmd\git.exe"
set "GIT_TERMINAL_PROMPT=0"

echo. >> "%LOG%"
echo ===== %DATE% %TIME% ===== >> "%LOG%"

if not exist "%PY%" (
  echo [FATAL] python.exe not found: %PY% >> "%LOG%"
  endlocal & exit /b 9009
)

REM --- 0. git self-heal: clear ALL stale *.lock, drop half-done rebase/merge,
REM        resync local branch to origin/main (mlb_two.csv owned by cloud dept1) ---
if exist "%GIT%" (
  for /r ".git" %%L in (*.lock) do del /f /q "%%L" >nul 2>&1
  "%GIT%" rebase --abort >nul 2>&1
  "%GIT%" merge --abort >nul 2>&1
  "%GIT%" fetch origin main --quiet >> "%LOG%" 2>&1
  "%GIT%" reset --mixed origin/main >> "%LOG%" 2>&1
  REM 2026-09-02 fix: do NOT restore root from stale origin copy.
  REM   Restoring root triggered a false CSVBACK "results lost" alarm.
  REM   Instead copy canonical data\mlb_two.csv to root. Canonical lives in data/.
  REM   ASCII-only on purpose: Korean in a bat mojibakes under cp949 and its
  REM   parens break this if() block - that broke 9/2 and 9/3 runs.
  copy /Y "data\mlb_two.csv" "mlb_two.csv" >nul 2>&1
)

REM --- 1. collect ---
"%PY%" "mlb_king.py" >> "%LOG%" 2>&1
set RC=%ERRORLEVEL%
if not "%RC%"=="0" (
  echo [FAIL] mlb_king.py exit code %RC% >> "%LOG%"
  endlocal & exit /b %RC%
)

REM --- 2. verify MLB_KING.csv DATE == today EDT (catch stale/overwrite failures) ---
"%PY%" -c "import csv,sys,io;from datetime import datetime,timedelta,timezone;edt=(datetime.now(tz=timezone.utc)+timedelta(hours=-4)).strftime('%%Y-%%m-%%d');raw=open('MLB_KING.csv','rb').read().replace(b'\x00',b'').decode('utf-8-sig',errors='replace');rows=list(csv.DictReader(io.StringIO(raw)));d=rows[0]['DATE'] if rows else 'EMPTY';print('[VERIFY] rows=%%d csv_date=%%s expected_edt=%%s'%%(len(rows),d,edt));sys.exit(0 if (rows and d==edt) else 9)" >> "%LOG%" 2>&1
set VRC=%ERRORLEVEL%
if not "%VRC%"=="0" (
  echo [FAIL] MLB_KING.csv date mismatch with today EDT or empty - dept1/dept3 will HALT. >> "%LOG%"
  echo [FAIL] manual check: python mlb_king.py >> "%LOG%"
  endlocal & exit /b %VRC%
)

REM --- 3. verify dept3 input files exist ---
for %%F in (dept3-1.json dept3-2.json mlb_king.json) do (
  if not exist "%%F" (
    echo [FAIL] %%F not generated - dept3 input missing >> "%LOG%"
    endlocal & exit /b 10
  )
)

echo [OK] MLB_KING.csv + dept3-1/2.json generated, date verified >> "%LOG%"
endlocal & exit /b 0
