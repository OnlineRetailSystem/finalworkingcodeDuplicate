@echo off
REM ============================================================
REM Kafka Event Flow Test Script (Windows)
REM Tests the complete event-driven architecture
REM ============================================================

echo ================================================================
echo          KAFKA EVENT FLOW INTEGRATION TEST
echo ================================================================
echo.

set API_GATEWAY=http://localhost:8090
set NOTIFICATION_SERVICE=http://localhost:8089

set TESTS_PASSED=0
set TESTS_FAILED=0

echo ================================================================
echo TEST 1: User Registration Event Flow
echo ================================================================
echo Testing: Auth Service -^> Kafka -^> Notification Service
echo.

REM Generate unique username
set TIMESTAMP=%time:~0,2%%time:~3,2%%time:~6,2%
set TEST_USER=testuser_%TIMESTAMP%
set TEST_EMAIL=test_%TIMESTAMP%@example.com

echo Registering user: %TEST_USER%
curl -X POST "%API_GATEWAY%/auth/signup" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"Test@123\", \"email\": \"%TEST_EMAIL%\", \"firstName\": \"Test\", \"lastName\": \"User\"}"

echo.
echo Waiting for Kafka event processing (5 seconds)...
timeout /t 5 /nobreak >nul

echo.
echo Checking notification service logs for USER_REGISTERED event...
echo Expected: Welcome email notification in logs
echo.

echo ================================================================
echo TEST 2: User Login Event Flow
echo ================================================================
echo Testing: Auth Service -^> Kafka -^> Notification Service
echo.

echo Logging in user: %TEST_USER%
curl -X POST "%API_GATEWAY%/auth/signin" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"Test@123\"}"

echo.
echo Waiting for Kafka event processing (5 seconds)...
timeout /t 5 /nobreak >nul

echo.
echo Checking notification service logs for USER_LOGGED_IN event...
echo Expected: Login alert notification in logs
echo.

echo ================================================================
echo TEST 3: Verify Processed Events (Idempotency)
echo ================================================================
echo Checking notification service processed events...
echo.

curl -s "%NOTIFICATION_SERVICE%/notifications/processed-events"

echo.
echo.

echo ================================================================
echo MANUAL VERIFICATION STEPS
echo ================================================================
echo.
echo 1. Check Notification Service Logs:
echo    docker logs ecom-notificationservice -f
echo.
echo 2. Check Auth Service Logs:
echo    docker logs ecom-authservice -f
echo.
echo 3. Check Order Service Logs:
echo    docker logs ecom-orderservice -f
echo.
echo 4. Check Payment Service Logs:
echo    docker logs ecom-paymentservice -f
echo.
echo 5. List Kafka Topics:
echo    docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092
echo.
echo 6. Monitor Kafka Messages (example for USER_REGISTERED):
echo    docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_REGISTERED --from-beginning
echo.

echo ================================================================
echo                TEST EXECUTION COMPLETE
echo ================================================================
pause
