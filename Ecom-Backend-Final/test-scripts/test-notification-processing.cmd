@echo off
REM ============================================================
REM Notification Service Processing Test
REM Verifies that notification service consumes and processes events
REM ============================================================

echo ================================================================
echo       NOTIFICATION SERVICE PROCESSING TEST
echo ================================================================
echo.

set API_GATEWAY=http://localhost:8090
set NOTIFICATION_SERVICE=http://localhost:8089

echo [STEP 1] Checking if notification service is running...
curl -s %NOTIFICATION_SERVICE%/notifications/health
if errorlevel 1 (
    echo.
    echo ERROR: Notification service is not responding!
    echo Please check: docker logs ecom-notificationservice
    pause
    exit /b 1
)
echo.
echo Notification service is healthy!
echo.

echo [STEP 2] Checking current processed events...
echo.
curl -s %NOTIFICATION_SERVICE%/notifications/processed-events
echo.
echo.

echo [STEP 3] Opening notification service logs in new window...
start "Notification Service Logs" cmd /k "docker logs ecom-notificationservice -f"
echo.
echo Log window opened. Keep it visible to see real-time processing.
echo.
timeout /t 3 /nobreak >nul

echo [STEP 4] Triggering USER_REGISTERED event...
echo.

set TIMESTAMP=%time:~0,2%%time:~3,2%%time:~6,2%%random%
set TEST_USER=notiftest_%TIMESTAMP%
set TEST_EMAIL=notiftest_%TIMESTAMP%@example.com

echo Registering user: %TEST_USER%
echo Email: %TEST_EMAIL%
echo.

curl -X POST "%API_GATEWAY%/auth/signup" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"Test@123\", \"email\": \"%TEST_EMAIL%\", \"firstName\": \"Notification\", \"lastName\": \"Test\"}"

echo.
echo.
echo User registered! Waiting for Kafka event processing...
echo.

echo [STEP 5] Waiting for notification service to process event (10 seconds)...
timeout /t 10 /nobreak

echo.
echo [STEP 6] Checking if event was processed...
echo.
echo Looking for event in processed_events table...
curl -s %NOTIFICATION_SERVICE%/notifications/processed-events | findstr /C:"%TEST_USER%" /C:"USER_REGISTERED"
if errorlevel 1 (
    echo.
    echo WARNING: Event not found in processed_events yet.
    echo Check the log window for processing details.
) else (
    echo.
    echo SUCCESS: Event was processed by notification service!
)
echo.

echo [STEP 7] Triggering USER_LOGGED_IN event...
echo.
echo Logging in user: %TEST_USER%
echo.

curl -X POST "%API_GATEWAY%/auth/signin" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"Test@123\"}"

echo.
echo.
echo User logged in! Waiting for Kafka event processing...
echo.

echo [STEP 8] Waiting for notification service to process login event (10 seconds)...
timeout /t 10 /nobreak

echo.
echo [STEP 9] Checking processed events again...
echo.
curl -s %NOTIFICATION_SERVICE%/notifications/processed-events
echo.
echo.

echo ================================================================
echo                    VERIFICATION CHECKLIST
echo ================================================================
echo.
echo Check the "Notification Service Logs" window for:
echo.
echo 1. USER_REGISTERED event:
echo    ╔══════════════════════════════════════════════════════════════╗
echo    ║  📧 NOTIFICATION: Welcome Email                            ║
echo    ║  To: %TEST_USER% (%TEST_EMAIL%)
echo    ╚══════════════════════════════════════════════════════════════╝
echo.
echo 2. USER_LOGGED_IN event:
echo    ╔══════════════════════════════════════════════════════════════╗
echo    ║  🔑 NOTIFICATION: Login Alert                              ║
echo    ║  User: %TEST_USER% has logged in
echo    ╚══════════════════════════════════════════════════════════════╝
echo.

echo ================================================================
echo                    MANUAL VERIFICATION
echo ================================================================
echo.
echo To verify Kafka topics were created:
echo   docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092
echo.
echo To see messages in USER_REGISTERED topic:
echo   docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_REGISTERED --from-beginning
echo.
echo To see messages in USER_LOGGED_IN topic:
echo   docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_LOGGED_IN --from-beginning
echo.
echo To check notification service logs:
echo   docker logs ecom-notificationservice --tail 50
echo.
echo To check auth service logs (producer):
echo   docker logs ecom-authservice --tail 50
echo.

echo ================================================================
echo                    TEST COMPLETE
echo ================================================================
echo.
echo Summary:
echo   - User registered: %TEST_USER%
echo   - Events triggered: USER_REGISTERED, USER_LOGGED_IN
echo   - Check log window for notification processing
echo   - Check processed_events endpoint for confirmation
echo.
pause
