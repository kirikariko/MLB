@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================================
echo  MLB 저장소 커밋 (2026-08-27 작업분)
echo ============================================================
echo.

REM lock 파일이 남아 있으면 커밋이 막힌다. 다른 git 창이 없으면 지워도 안전하다.
if exist ".git\index.lock" (
  echo [1/4] 잔재 lock 제거...
  del /f /q ".git\index.lock"
)
if exist ".git\HEAD.lock" del /f /q ".git\HEAD.lock"

echo [2/4] 변경분 확인...
git add -A
git status --short | more

echo.
echo [3/4] 커밋...
git -c core.quotepath=false commit -m "브랜드 사명 최상위 조항 + I-13/I-14 수리 + 루트 정리" -m "사명: '정직, 성실하게 유저들의 베팅을 더 이롭게' 를 CLAUDE.md 최상단과 5개 부서 SKILL.md 전부에 박음. 모든 HARD HALT 위에 두고, 충돌 시 사명이 이긴다." -m "I-13 (8/26 X 미게시): dept5가 산출물을 dept5/ -> dept5/trackA/ 로 옮겼으나 x_poster는 옛 경로만 확인. 파일이 있는데도 SKIP으로 하루 게시 유실. resolve_artifact() 3단 사다리(매니페스트->파일탐색->정직포기) 도입. dept5 _DONE.json artifacts 매니페스트 의무화. 9.0-ARTIFACT 신설, 9.0-XPOST는 로그 항목 부재도 실패로 판정. 미게시도 posted:false 기록. dry-run은 로그 미오염." -m "I-14 (학습데이터 후퇴): 8/24 결과 10경기가 커밋 1afd8324 에는 있으나 디스크에서만 50셀 소실. 원인 미특정 - 추정 기재 안 함. 커밋본에서 복구 + 8/25 15경기 신규 기입. 9.0-REGRESS 신설(결과 채움 최고치가 줄면 차단)." -m "기타: twofold_tracker 8/22 HOU_OAK HIT->MISS (I-10 잔재). boxscore_review 이닝표기(5.2=5와3분의2)/블론세이브/승패 판정 수리. dept1 SKILL.md UTF-8 절단 복구(유실 문장은 추정하지 않고 표시). 루트 267개 파일을 _archive/ 카테고리별 이동 - 참조 전수조사 후 미참조분만."

if errorlevel 1 (
  echo.
  echo [!] 커밋 실패. 위 메시지를 확인하세요.
) else (
  echo.
  echo [4/4] 커밋 완료.
  git log --oneline -3
)

echo.
echo 아무 키나 누르면 닫힙니다.
pause >nul
