@echo off
REM ============================================================
REM  register_mlb_king_task.bat — 한 번만 더블클릭하면 등록 완료
REM
REM  Windows 작업 스케줄러에 "MLB_KING_Daily" 작업을 만든다.
REM  매일 16:30 (PC 로컬시각 = KST) 실행.
REM  관리자 권한 필요 없음 (현재 사용자 작업으로 등록).
REM ============================================================
chcp 65001 > nul
setlocal
cd /d "%~dp0.."
set "TASKNAME=MLB_KING_Daily"
set "TARGET=%~dp0run_mlb_king.bat"

echo ============================================================
echo  MLB KING 일일 자동 수집 등록
echo ------------------------------------------------------------
echo  작업 이름 : %TASKNAME%
echo  실행 파일 : %TARGET%
echo  실행 시각 : 매일 16:30 (dept1 17:30보다 1시간 앞)
echo ============================================================
echo.

schtasks /query /tn "%TASKNAME%" >nul 2>&1
if %ERRORLEVEL%==0 (
  echo [i] 같은 이름의 작업이 이미 있습니다. 새 설정으로 덮어씁니다.
  echo.
)

schtasks /create /tn "%TASKNAME%" /tr "\"%TARGET%\"" /sc daily /st 16:30 /rl LIMITED /f
if not "%ERRORLEVEL%"=="0" (
  echo.
  echo [X] 등록 실패. 위 오류 메시지를 책임자에게 보여주세요.
  pause
  exit /b 1
)

echo.
echo [OK] 등록 완료.
echo.
echo --- 등록 내용 확인 ---
schtasks /query /tn "%TASKNAME%" /v /fo LIST | findstr /i "TaskName Next Status Task_To_Run Schedule Start"
echo.
echo ------------------------------------------------------------
echo  지금 바로 한 번 테스트 실행하시겠습니까?
echo  (오늘 데이터를 다시 수집합니다. 안전합니다.)
echo ------------------------------------------------------------
choice /c YN /m "테스트 실행 (Y=예 / N=아니오)"
if errorlevel 2 goto :done
schtasks /run /tn "%TASKNAME%"
echo.
echo 실행 요청됨. 결과는 logs\mlb_king.log 에서 확인하세요.

:done
echo.
echo 참고: 작업 삭제는  schtasks /delete /tn "%TASKNAME%" /f
pause
endlocal
