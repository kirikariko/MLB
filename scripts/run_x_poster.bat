@echo off
REM ============================================================
REM  run_x_poster.bat — Windows 작업 스케줄러용 X 게시 실행기
REM  매일 KST 22:00 실행 (dept5 트랙A 21:32 완료 30분 뒤)
REM  Claude 앱/Chrome 꺼져 있어도 독립 동작
REM ============================================================
setlocal
cd /d "%~dp0.."
if not exist "logs" mkdir "logs"

echo. >> "logs\x_poster.log"
echo ===== %DATE% %TIME% ===== >> "logs\x_poster.log"

python "scripts\x_poster.py" >> "logs\x_poster.log" 2>&1
set RC=%ERRORLEVEL%

if not "%RC%"=="0" (
  echo [WARN] exit code %RC% >> "logs\x_poster.log"
)

endlocal & exit /b %RC%
