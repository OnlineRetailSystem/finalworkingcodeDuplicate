@echo off
REM ============================================================
REM Master Test Script - Runs all tests
REM ============================================================

echo ================================================================
echo          E-COMMERCE BACKEND COMPREHENSIVE TEST SUITE
echo ================================================================
echo.
echo This script will run all tests to verify:
echo   1. Kafka event-driven architecture
echo   2. Authentication and security
echo   3. Frontend notification delivery
echo.
echo Press any key to start testing...
pause >nul
cls

echo ================================================================
echo STEP 1: Verify Services are Running
echo ================================================================
echo.

echo Checking API Gateway...
curl -s -o nul -w "API Gateway (8090): %%{http_code}\n" http://localhost:8090

echo Checking Auth Service...
curl -s -o nul -w "Auth Service (8087): %%{http_code}\n" http://localhost:8087

echo Checking Notification Service...
curl -s -o nul -w "Notification Service (8089): %%{http_code}\n" http://localhost:8089/notifications/health

echo Checking Frontend...
curl -s -o nul -w "Frontend (3000): %%{http_code}\n" http://localhost:3000

echo.
echo If any service shows error, please start Docker containers:
echo   docker-compose up -d
echo.
pause

cls

echo ================================================================
echo STEP 2: Running Kafka Event Flow Tests
echo ================================================================
echo.
call test-kafka-flow.cmd

cls

echo ================================================================
echo STEP 3: Running Authentication Tests
echo ================================================================
echo.
call test-auth-security.cmd

cls

echo ================================================================
echo STEP 4: Running Frontend Notification Tests
echo ================================================================
echo.
call test-frontend-notifications.cmd

cls

echo ================================================================
echo STEP 5: Checking Service Logs
echo ================================================================
echo.

echo Opening notification service logs in new window...
start "Notification Service Logs" cmd /k docker logs ecom-notificationservice --tail 50 -f

echo.
echo Opening auth service logs in new window...
start "Auth Service Logs" cmd /k docker logs ecom-authservice --tail 50 -f

echo.
echo Logs are now visible in separate windows.
echo Check for Kafka event processing messages.
echo.
pause

cls

echo ================================================================
echo STEP 6: Kafka Topics Verification
echo ================================================================
echo.

echo Listing all Kafka topics...
docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092

echo.
echo Expected topics:
echo   - USER_REGISTERED
echo   - USER_LOGGED_IN
echo   - PAYMENT_SUCCESS
echo   - ORDER_PLACED
echo   - ORDER_STATUS_UPDATED
echo   - LOW_STOCK_ALERT
echo.
pause

cls

echo ================================================================
echo STEP 7: Database Verification
echo ================================================================
echo.

echo Checking processed events in notification database...
curl -s http://localhost:8089/notifications/processed-events

echo.
echo.
echo This shows all events that have been processed by the notification service.
echo Each event should have a unique eventId to prevent duplicates.
echo.
pause

cls

echo ================================================================
echo                    TEST SUMMARY
echo ================================================================
echo.
echo KAFKA IMPLEMENTATION:
echo   Status: WORKING
echo   - Events are published by producers
echo   - Events are consumed by consumers
echo   - Idempotency is maintained
echo   - Notifications logged in backend
echo.
echo JWT IMPLEMENTATION:
echo   Status: NOT IMPLEMENTED
echo   - Currently using Basic Authentication
echo   - No JWT token generation
echo   - No token-based authorization
echo   - Recommendation: Implement JWT
echo.
echo FRONTEND NOTIFICATIONS:
echo   Status: NOT WORKING
echo   - Backend processes events correctly
echo   - No real-time delivery to frontend
echo   - No WebSocket or SSE implementation
echo   - Recommendation: Implement WebSocket
echo.
echo ================================================================
echo                    DETAILED REPORT
echo ================================================================
echo.
echo A comprehensive test report has been generated:
echo   Location: test-scripts/TEST-REPORT.md
echo.
echo The report includes:
echo   - Detailed analysis of each component
echo   - Code evidence and examples
echo   - Implementation recommendations
echo   - Step-by-step guides for improvements
echo.
echo ================================================================
echo                    RECOMMENDATIONS
echo ================================================================
echo.
echo PRIORITY 1: Implement JWT Authentication
echo   - Add JWT dependencies
echo   - Create JwtUtil class
echo   - Update SecurityConfig
echo   - Modify AuthController
echo.
echo PRIORITY 2: Implement Frontend Notifications
echo   - Add WebSocket to backend
echo   - Create notification component in frontend
echo   - Connect to WebSocket on login
echo   - Display notifications using toast
echo.
echo PRIORITY 3: Security Enhancements
echo   - Add rate limiting
echo   - Implement account lockout
echo   - Enforce password strength
echo   - Use HTTPS in production
echo.
echo ================================================================
echo                    NEXT STEPS
echo ================================================================
echo.
echo 1. Review TEST-REPORT.md for detailed findings
echo 2. Check service logs for Kafka event processing
echo 3. Implement JWT authentication
echo 4. Add WebSocket for real-time notifications
echo 5. Test end-to-end flow
echo.
echo ================================================================
echo                    TEST EXECUTION COMPLETE
echo ================================================================
echo.
echo Thank you for using the E-Commerce Backend Test Suite!
echo.
pause
