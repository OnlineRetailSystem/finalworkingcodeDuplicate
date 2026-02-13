@echo off
REM ============================================================
REM System Status Check Script
REM Quick health check for all services
REM ============================================================

echo ================================================================
echo          E-COMMERCE SYSTEM STATUS CHECK
echo ================================================================
echo.

set ALL_HEALTHY=1

echo Checking Docker containers...
echo.
docker ps --filter "name=ecom-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo.

echo ================================================================
echo Service Health Checks
echo ================================================================
echo.

echo [1/10] Checking MySQL...
curl -s -o nul -w "MySQL (3307): %%{http_code}\n" http://localhost:3307 2>nul
if errorlevel 1 (
    echo MySQL: Cannot connect
    set ALL_HEALTHY=0
) else (
    echo MySQL: Running
)

echo.
echo [2/10] Checking Zookeeper...
docker exec ecom-zookeeper bash -c "echo ruok | nc localhost 2181" 2>nul
if errorlevel 1 (
    echo Zookeeper: Not responding
    set ALL_HEALTHY=0
) else (
    echo Zookeeper: Healthy
)

echo.
echo [3/10] Checking Kafka...
docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092 >nul 2>&1
if errorlevel 1 (
    echo Kafka: Not responding
    set ALL_HEALTHY=0
) else (
    echo Kafka: Healthy
)

echo.
echo [4/10] Checking Eureka Server...
curl -s -o nul -w "Eureka (8761): %%{http_code}\n" http://localhost:8761
if errorlevel 1 (
    echo Eureka: Cannot connect
    set ALL_HEALTHY=0
)

echo.
echo [5/10] Checking API Gateway...
curl -s -o nul -w "API Gateway (8090): %%{http_code}\n" http://localhost:8090
if errorlevel 1 (
    echo API Gateway: Cannot connect
    set ALL_HEALTHY=0
)

echo.
echo [6/10] Checking Auth Service...
curl -s -o nul -w "Auth Service (8087): %%{http_code}\n" http://localhost:8087
if errorlevel 1 (
    echo Auth Service: Cannot connect
    set ALL_HEALTHY=0
)

echo.
echo [7/10] Checking Product Service...
curl -s -o nul -w "Product Service (8082): %%{http_code}\n" http://localhost:8082
if errorlevel 1 (
    echo Product Service: Cannot connect
    set ALL_HEALTHY=0
)

echo.
echo [8/10] Checking Order Service...
curl -s -o nul -w "Order Service (8083): %%{http_code}\n" http://localhost:8083
if errorlevel 1 (
    echo Order Service: Cannot connect
    set ALL_HEALTHY=0
)

echo.
echo [9/10] Checking Notification Service...
curl -s http://localhost:8089/notifications/health
if errorlevel 1 (
    echo Notification Service: Cannot connect
    set ALL_HEALTHY=0
)

echo.
echo [10/10] Checking Frontend...
curl -s -o nul -w "Frontend (3000): %%{http_code}\n" http://localhost:3000
if errorlevel 1 (
    echo Frontend: Cannot connect
    set ALL_HEALTHY=0
)

echo.
echo ================================================================
echo Kafka Topics
echo ================================================================
echo.
docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092 2>nul
if errorlevel 1 (
    echo Cannot list Kafka topics
    set ALL_HEALTHY=0
)

echo.
echo ================================================================
echo Registered Services (Eureka)
echo ================================================================
echo.
echo Opening Eureka Dashboard: http://localhost:8761
echo Check if all services are registered
echo.

echo ================================================================
echo Recent Notification Service Logs
echo ================================================================
echo.
docker logs ecom-notificationservice --tail 10 2>nul
if errorlevel 1 (
    echo Cannot retrieve logs
)

echo.
echo ================================================================
echo System Status Summary
echo ================================================================
echo.

if %ALL_HEALTHY%==1 (
    echo Status: ALL SYSTEMS OPERATIONAL
    echo.
    echo You can now run the test scripts:
    echo   - run-all-tests.cmd
    echo   - test-kafka-flow.cmd
    echo   - test-auth-security.cmd
    echo   - test-frontend-notifications.cmd
) else (
    echo Status: SOME SERVICES ARE DOWN
    echo.
    echo Please start the services:
    echo   cd finalworkingcodeDuplicate/Ecom-Backend-Final
    echo   docker-compose up -d
    echo.
    echo Wait 2-3 minutes for all services to start, then run this script again.
)

echo.
echo ================================================================
echo Quick Actions
echo ================================================================
echo.
echo View all logs:        docker-compose logs -f
echo Restart all:          docker-compose restart
echo Stop all:             docker-compose down
echo Start all:            docker-compose up -d
echo.
echo View specific service logs:
echo   docker logs ecom-notificationservice -f
echo   docker logs ecom-authservice -f
echo   docker logs ecom-orderservice -f
echo.

pause
