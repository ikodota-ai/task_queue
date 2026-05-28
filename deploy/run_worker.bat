@echo off
REM IG/X Crawler Worker — 自动重启 (maxpage 由入队时指定，worker 不用传)
REM 用法: 在任意目录运行 deploy\run_worker.bat 即可
REM       deploy\run_worker.bat                  (默认: ig_crawler.py --mode all)
REM       deploy\run_worker.bat full             (仅全量)
REM       deploy\run_worker.bat incr             (仅增量)
REM       deploy\run_worker.bat all x_crawler    (X 平台)
REM       deploy\run_worker.bat incr ig_crawler 0 (增量, 不限任务数)

cd /d "%~dp0.."

set MODE=%1
if "%MODE%"=="" set MODE=all

set SCRIPT=%2
if "%SCRIPT%"=="" set SCRIPT=ig_crawler

set MAX_TASKS=%3
if "%MAX_TASKS%"=="" set MAX_TASKS=20

set PYTHON=venv\Scripts\python.exe

if not exist "%PYTHON%" (
    echo ERROR: %PYTHON% not found, run install.sh first
    exit /b 1
)

set /a COUNT=0

:loop
set /a COUNT+=1
echo ========================================
echo [%DATE% %TIME%] Worker #%COUNT% starting: %PYTHON% -u %SCRIPT%.py --mode %MODE% (MAX_TASKS=%MAX_TASKS%)
echo ========================================

set MAX_TASKS_PER_WORKER=%MAX_TASKS%
"%PYTHON%" -u %SCRIPT%.py --mode %MODE%

echo [%DATE% %TIME%] Worker #%COUNT% exited (code: %ERRORLEVEL%)
echo Press Ctrl+C to stop, or wait 3s to restart...
choice /t 3 /d y /n >nul
if errorlevel 2 goto :eof
goto loop
