@echo off
REM ============================================================
REM Quick Test Launcher - Just double-click this file!
REM ============================================================

echo ================================================================
echo          E-COMMERCE BACKEND TEST LAUNCHER
echo ================================================================
echo.
echo This will run all tests for your backend system.
echo.
echo What will be tested:
echo   1. Kafka event-driven architecture
echo   2. JWT authentication
echo   3. Frontend notifications
echo.
echo Location: %~dp0
echo.
echo Press any key to continue or close this window to cancel...
pause >nul

REM Change to test-scripts directory
cd /d "%~dp0test-scripts"

REM Check if test-scripts folder exists
if not exist "run-all-tests.cmd" (
    echo.
    echo ERROR: Cannot find test-scripts folder!
    echo Please make sure you're running this from the correct location.
    echo.
    pause
    exit /b 1
)

REM Run all tests
call run-all-tests.cmd

echo.
echo ================================================================
echo Tests complete! Check the results above.
echo.
echo To read detailed reports:
echo   - QUICK-SUMMARY.md (quick overview)
echo   - TEST-REPORT.md (detailed analysis)
echo.
echo These files are in the test-scripts folder.
echo ================================================================
echo.
pause
