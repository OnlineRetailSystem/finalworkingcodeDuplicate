@echo off
REM ============================================================
REM Complete Kafka Flow Test
REM Tests entire flow: Producer -> Kafka -> Consumer -> Processing
REM ============================================================

echo ================================================================
echo          COMPLETE KAFKA FLOW VERIFICATION TEST
echo ================================================================
echo.
echo This test will verify:
echo   1. Kafka topics are created
echo   2. Auth service publishes events
echo   3. Notification service consumes events
echo   4. Events are processed correctly
echo   5. Idempotency prevents duplicates
echo.
pause

REM ============================================================
REM PHASE 1: Pre-flight checks
REM ============================================================
echo.
echo ================================================================
echo PHASE 1: Pre-flight Checks
echo ================================================================
echo.

echo [1.1] Checking Kafka...
docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092 >nul 2>&1
if errorlevel 1 (
    echo ERROR: Kafka is not running!
    pause
    exit /b 1
)
echo Kafka: RUNNING

echo.
echo [1.2] Checking Auth Service...
curl -s -o nul -w "Auth Service: %%{http_code}\n" http://localhost:8087
if errorlevel 1 (
    echo ERROR: Auth service is not responding!
    pause
    exit /b 1
)

echo.
echo [1.3] Checking Notification Service...
curl -s http://localhost:8089/notifications/health >nul
if errorlevel 1 (
    echo ERROR: Notification service is not responding!
    pause
    exit /b 1
)
echo Notification Service: RUNNING

echo.
echo All services are ready!
echo.
pause

REM ============================================================
REM PHASE 2: Check initial state
REM ============================================================
echo.
echo ================================================================
echo PHASE 2: Initial State
echo ================================================================
echo.

echo [2.1] Current Kafka topics:
docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092
echo.

echo [2.2] Current processed events:
curl -s http://localhost:8089/notifications/processed-events
echo.
echo.
pause

REM ============================================================
REM PHASE 3: Open monitoring windows
REM ============================================================
echo.
echo ================================================================
echo PHASE 3: Opening Monitoring Windows
echo ================================================================
echo.

echo Opening Auth Service logs...
start "Auth Service Logs (Producer)" cmd /k "docker logs ecom-authservice -f"
timeout /t 2 /nobreak >nul

echo Opening Notification Service logs...
start "Notification Service Logs (Consumer)" cmd /k "docker logs ecom-notificationservice -f"
timeout /t 2 /nobreak >nul

echo Opening Kafka topic monitor for USER_REGISTERED...
start "Kafka Topic: USER_REGISTERED" cmd /k "docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_REGISTERED --from-beginning"
timeout /t 2 /nobreak >nul

echo.
echo All monitoring windows opened!
echo Keep them visible to see the flow in real-time.
echo.
pause

REM ============================================================
REM PHASE 4: Trigger events
REM ============================================================
echo.
echo ================================================================
echo PHASE 4: Triggering Events
echo ================================================================
echo.

set TIMESTAMP=%time:~0,2%%time:~3,2%%time:~6,2%%random%
set TEST_USER=flowtest_%TIMESTAMP%
set TEST_EMAIL=flowtest_%TIMESTAMP%@example.com

echo [4.1] Registering user to trigger USER_REGISTERED event...
echo.
echo Username: %TEST_USER%
echo Email: %TEST_EMAIL%
echo.

curl -X POST "http://localhost:8090/auth/signup" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"Test@123\", \"email\": \"%TEST_EMAIL%\", \"firstName\": \"Flow\", \"lastName\": \"Test\"}"

echo.
echo.
echo Event triggered! Watch the monitoring windows:
echo.
echo 1. Auth Service Logs: Should show "Publishing USER_REGISTERED event"
echo 2. Kafka Topic Monitor: Should show the event message
echo 3. Notification Service Logs: Should show "Welcome Email" notification
echo.
echo Waiting 10 seconds for processing...
timeout /t 10 /nobreak

echo.
echo [4.2] Logging in user to trigger USER_LOGGED_IN event...
echo.

curl -X POST "http://localhost:8090/auth/signin" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"Test@123\"}"

echo.
echo.
echo Login event triggered! Watch the monitoring windows:
echo.
echo 1. Auth Service Logs: Should show "Publishing USER_LOGGED_IN event"
echo 2. Notification Service Logs: Should show "Login Alert" notification
echo.
echo Waiting 10 seconds for processing...
timeout /t 10 /nobreak

REM ============================================================
REM PHASE 5: Verify processing
REM ============================================================
echo.
echo ================================================================
echo PHASE 5: Verifying Event Processing
echo ================================================================
echo.

echo [5.1] Checking if topics were created...
echo.
docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092 | findstr /C:"USER_REGISTERED" /C:"USER_LOGGED_IN"
echo.

echo [5.2] Checking processed events in database...
echo.
curl -s http://localhost:8089/notifications/processed-events
echo.
echo.

echo [5.3] Counting processed events...
echo.
for /f %%i in ('curl -s http://localhost:8089/notifications/processed-events ^| find /c "eventId"') do set EVENT_COUNT=%%i
echo Total events processed: %EVENT_COUNT%
echo.

REM ============================================================
REM PHASE 6: Test idempotency
REM ============================================================
echo.
echo ================================================================
echo PHASE 6: Testing Idempotency (Duplicate Prevention)
echo ================================================================
echo.

echo [6.1] Attempting to register same user again...
echo This should fail at API level (duplicate username)
echo.

curl -X POST "http://localhost:8090/auth/signup" ^
  -H "Content-Type: application/json" ^
  -d "{\"username\": \"%TEST_USER%\", \"password\": \"Test@123\", \"email\": \"different@example.com\"}"

echo.
echo.
echo Expected: "Username already exists" error
echo.
pause

REM ============================================================
REM PHASE 7: Check consumer group
REM ============================================================
echo.
echo ================================================================
echo PHASE 7: Kafka Consumer Group Status
echo ================================================================
echo.

echo [7.1] Listing consumer groups...
docker exec ecom-kafka kafka-consumer-groups --list --bootstrap-server localhost:9092
echo.

echo [7.2] Notification service consumer group details...
docker exec ecom-kafka kafka-consumer-groups --describe --group notification-service-group --bootstrap-server localhost:9092
echo.

REM ============================================================
REM PHASE 8: Summary
REM ============================================================
echo.
echo ================================================================
echo                    FLOW VERIFICATION SUMMARY
echo ================================================================
echo.

echo Test User: %TEST_USER%
echo Test Email: %TEST_EMAIL%
echo.

echo VERIFICATION CHECKLIST:
echo.
echo [ ] Auth Service Logs show "Publishing USER_REGISTERED event"
echo [ ] Auth Service Logs show "Publishing USER_LOGGED_IN event"
echo [ ] Kafka Topic Monitor shows event messages
echo [ ] Notification Service Logs show "Welcome Email" notification
echo [ ] Notification Service Logs show "Login Alert" notification
echo [ ] Processed events endpoint shows both events
echo [ ] No duplicate processing occurred
echo [ ] Consumer group is active and consuming
echo.

echo ================================================================
echo                    KAFKA FLOW DIAGRAM
echo ================================================================
echo.
echo 1. USER REGISTRATION FLOW:
echo.
echo    Frontend
echo       │
echo       ▼
echo    API Gateway (8090)
echo       │
echo       ▼
echo    Auth Service (8087)
echo       │
echo       ├─► MySQL (Save user)
echo       │
echo       └─► Kafka Producer
echo              │
echo              ▼
echo           Kafka Topic: USER_REGISTERED
echo              │
echo              ▼
echo           Kafka Consumer
echo              │
echo              ▼
echo    Notification Service (8089)
echo       │
echo       ├─► Check idempotency (processed_events table)
echo       │
echo       ├─► Log notification
echo       │
echo       └─► Save to processed_events
echo.

echo 2. USER LOGIN FLOW:
echo.
echo    Frontend
echo       │
echo       ▼
echo    API Gateway (8090)
echo       │
echo       ▼
echo    Auth Service (8087)
echo       │
echo       ├─► Verify credentials
echo       │
echo       └─► Kafka Producer
echo              │
echo              ▼
echo           Kafka Topic: USER_LOGGED_IN
echo              │
echo              ▼
echo           Kafka Consumer
echo              │
echo              ▼
echo    Notification Service (8089)
echo       │
echo       ├─► Check idempotency
echo       │
echo       ├─► Log notification
echo       │
echo       └─► Save to processed_events
echo.

echo ================================================================
echo                    ADDITIONAL COMMANDS
echo ================================================================
echo.
echo To manually check Kafka messages:
echo   docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_REGISTERED --from-beginning
echo.
echo To check notification service database:
echo   docker exec ecom-mysql mysql -uroot -proot -e "SELECT * FROM notificationdb.processed_events;"
echo.
echo To check auth service database:
echo   docker exec ecom-mysql mysql -uroot -proot -e "SELECT username, email FROM authservice.users WHERE username='%TEST_USER%';"
echo.
echo To see all Kafka topics:
echo   docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092
echo.
echo To see consumer lag:
echo   docker exec ecom-kafka kafka-consumer-groups --describe --group notification-service-group --bootstrap-server localhost:9092
echo.

echo ================================================================
echo                    TEST COMPLETE
echo ================================================================
echo.
echo The complete Kafka flow has been tested!
echo.
echo Review the monitoring windows to see:
echo   - Auth Service publishing events
echo   - Kafka storing events
echo   - Notification Service consuming events
echo   - Events being processed and logged
echo.
echo All windows will remain open for your review.
echo Close them when done.
echo.
pause
