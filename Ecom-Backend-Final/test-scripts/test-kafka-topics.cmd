@echo off
REM ============================================================
REM Kafka Topics Verification Script
REM Checks if Kafka topics are created and accessible
REM ============================================================

echo ================================================================
echo          KAFKA TOPICS VERIFICATION TEST
echo ================================================================
echo.

echo [TEST 1] Checking if Kafka container is running...
docker ps --filter "name=ecom-kafka" --format "{{.Names}} - {{.Status}}"
if errorlevel 1 (
    echo ERROR: Kafka container is not running!
    echo Please start services: docker-compose up -d
    pause
    exit /b 1
)
echo.

echo [TEST 2] Listing all Kafka topics...
echo.
docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092
if errorlevel 1 (
    echo ERROR: Cannot connect to Kafka!
    pause
    exit /b 1
)
echo.

echo [TEST 3] Checking for expected topics...
echo.
echo Expected topics:
echo   - USER_REGISTERED
echo   - USER_LOGGED_IN
echo   - PAYMENT_SUCCESS
echo   - ORDER_PLACED
echo   - ORDER_STATUS_UPDATED
echo   - LOW_STOCK_ALERT
echo.

set TOPICS_FOUND=0

docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092 > temp_topics.txt 2>&1

findstr /C:"USER_REGISTERED" temp_topics.txt >nul
if %errorlevel%==0 (
    echo [FOUND] USER_REGISTERED
    set /a TOPICS_FOUND+=1
) else (
    echo [MISSING] USER_REGISTERED
)

findstr /C:"USER_LOGGED_IN" temp_topics.txt >nul
if %errorlevel%==0 (
    echo [FOUND] USER_LOGGED_IN
    set /a TOPICS_FOUND+=1
) else (
    echo [MISSING] USER_LOGGED_IN
)

findstr /C:"PAYMENT_SUCCESS" temp_topics.txt >nul
if %errorlevel%==0 (
    echo [FOUND] PAYMENT_SUCCESS
    set /a TOPICS_FOUND+=1
) else (
    echo [MISSING] PAYMENT_SUCCESS
)

findstr /C:"ORDER_PLACED" temp_topics.txt >nul
if %errorlevel%==0 (
    echo [FOUND] ORDER_PLACED
    set /a TOPICS_FOUND+=1
) else (
    echo [MISSING] ORDER_PLACED
)

findstr /C:"ORDER_STATUS_UPDATED" temp_topics.txt >nul
if %errorlevel%==0 (
    echo [FOUND] ORDER_STATUS_UPDATED
    set /a TOPICS_FOUND+=1
) else (
    echo [MISSING] ORDER_STATUS_UPDATED
)

findstr /C:"LOW_STOCK_ALERT" temp_topics.txt >nul
if %errorlevel%==0 (
    echo [FOUND] LOW_STOCK_ALERT
    set /a TOPICS_FOUND+=1
) else (
    echo [MISSING] LOW_STOCK_ALERT
)

del temp_topics.txt

echo.
echo Topics found: %TOPICS_FOUND% / 6
echo.

if %TOPICS_FOUND% LSS 6 (
    echo NOTE: Some topics are missing. They will be auto-created when first used.
    echo This is normal if no events have been published yet.
)

echo.
echo [TEST 4] Getting topic details...
echo.

echo --- USER_REGISTERED Topic Details ---
docker exec ecom-kafka kafka-topics --describe --topic USER_REGISTERED --bootstrap-server localhost:9092 2>nul
if errorlevel 1 (
    echo Topic not created yet. Will be auto-created on first event.
)
echo.

echo --- USER_LOGGED_IN Topic Details ---
docker exec ecom-kafka kafka-topics --describe --topic USER_LOGGED_IN --bootstrap-server localhost:9092 2>nul
if errorlevel 1 (
    echo Topic not created yet. Will be auto-created on first event.
)
echo.

echo [TEST 5] Checking Kafka consumer groups...
echo.
docker exec ecom-kafka kafka-consumer-groups --list --bootstrap-server localhost:9092
echo.

echo [TEST 6] Checking notification-service-group details...
echo.
docker exec ecom-kafka kafka-consumer-groups --describe --group notification-service-group --bootstrap-server localhost:9092 2>nul
if errorlevel 1 (
    echo Consumer group not active yet. Will be created when notification service starts consuming.
)
echo.

echo ================================================================
echo                    SUMMARY
echo ================================================================
echo.
echo Kafka Status: RUNNING
echo Topics Found: %TOPICS_FOUND% / 6
echo.
echo To trigger topic creation:
echo   1. Register a user (creates USER_REGISTERED topic)
echo   2. Login (creates USER_LOGGED_IN topic)
echo   3. Process payment (creates PAYMENT_SUCCESS topic)
echo   4. Create order (creates ORDER_PLACED topic)
echo.
echo To monitor topics in real-time:
echo   docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_REGISTERED --from-beginning
echo.
pause
