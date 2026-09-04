@echo off
chcp 65001 >nul
setlocal EnableDelayedExpansion
cd /d "%~dp0"
title MLB KING 즉시 실행

echo ============================================================
echo   MLB_KING 즉시 실행  (파이프라인이 멈췄을 때 이걸 누른다)
echo ============================================================
echo.
echo  이 스크립트가 하는 일:
echo    1. mlb_king.py 실행 - MLB StatsAPI + FanGraphs 에서 오늘 데이터 수집
echo    2. MLB_KING.csv 날짜가 오늘 EDT 와 맞는지 검증
echo    3. dept3 입력 파일 3종 생성 확인
echo.
echo  ※ 이건 사장님 PC 에서만 됩니다. Claude 샌드박스는 해당 API 가 막혀 있습니다.
echo.

if not exist "logs" mkdir "logs"
set "LOG=logs\mlb_king_manual.log"

for /f %%i in ('python -c "from datetime import datetime,timedelta,timezone;print((datetime.now(timezone.utc)+timedelta(hours=-4)).strftime('%%Y-%%m-%%d'))"') do set "EDT=%%i"
echo  오늘 EDT 슬레이트 = !EDT!
echo.

echo [1/3] mlb_king.py 실행 중... (2~5분 걸립니다)
echo ===== MANUAL RUN %DATE% %TIME% ===== >> "%LOG%"
python "mlb_king.py" >> "%LOG%" 2>&1
if errorlevel 1 (
  echo.
  echo  [실패] mlb_king.py 가 오류로 끝났습니다.
  echo         logs\mlb_king_manual.log 마지막 부분:
  echo  ----------------------------------------------------------
  powershell -NoProfile -Command "Get-Content '%LOG%' -Tail 15"
  echo  ----------------------------------------------------------
  goto :END
)
echo       완료.
echo.

echo [2/3] MLB_KING.csv 날짜 검증...
python -c "import csv,sys,io;from datetime import datetime,timedelta,timezone;edt=(datetime.now(timezone.utc)+timedelta(hours=-4)).strftime('%%Y-%%m-%%d');raw=open('MLB_KING.csv','rb').read().replace(b'\x00',b'').decode('utf-8-sig',errors='replace');rows=list(csv.DictReader(io.StringIO(raw)));d=rows[0]['DATE'] if rows else 'EMPTY';print('      %%d경기 / csv날짜=%%s / 기대=%%s'%%(len(rows),d,edt));sys.exit(0 if (rows and d==edt) else 9)"
if errorlevel 1 (
  echo.
  echo  [실패] MLB_KING.csv 가 오늘 날짜가 아닙니다.
  echo         어제 파일이 남아 있거나 덮어쓰기가 실패했습니다.
  goto :END
)
echo.

echo [3/3] dept3 입력 파일 확인...
set MISSING=0
for %%F in (dept3-1.json dept3-2.json mlb_king.json) do (
  if not exist "%%F" ( echo       [없음] %%F & set MISSING=1 ) else ( echo       [OK] %%F )
)
if "!MISSING!"=="1" ( echo. & echo  [실패] dept3 입력이 빠졌습니다. & goto :END )

echo.
echo ============================================================
echo   ✅ 성공 — KING 준비 완료
echo ============================================================
echo.
echo   다음 순서:
echo     - 정규 시각이 아직 안 지났으면 dept1(17:30)부터 자동으로 이어집니다.
echo     - 이미 지났으면 Claude 에게 "KING 올렸어" 라고 말하세요.
echo       dept1~dept3 를 수동으로 밀어 dept4 시각에 맞춥니다.
echo.

:END
echo.
echo 아무 키나 누르면 닫힙니다.
pause >nul
endlocal
