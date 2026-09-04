@echo off
cd /d C:\Users\AA\MLB
echo === MLB Syndicate DB Backfill ===
echo Date: 2026-05-18
echo.
python data\db_backfill_v2.py --date 2026-05-18
echo.
echo === Done ===
pause
