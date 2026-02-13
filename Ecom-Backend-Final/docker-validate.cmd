@echo off
REM Validate Docker setup and service health

echo ========================================
echo Docker Environment Validation
echo ========================================
echo.

echo Checking Docker...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker is not installed or not in PATH
    exit /b 1
) else (
    docker --version
)

echo.
echo Checking Docker Compose...
docker-compose --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker Compose is not installed or not in PATH
    exit /b 1
) else (
    docker-compose --version
)

echo.
echo Checking if Docker daemon is running...
docker ps >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Docker daemon is not running. Please start Docker Desktop.
    exit /b 1
) else (
    echo [OK] Docker daemon is running
)

echo.
echo ========================================
echo Service Status
echo ========================================
docker-compose ps

echo.
echo ========================================
echo Service Health Checks
echo ========================================

echo.
echo Checking Eureka Server...
curl -s http://localhost:8761/actuator/health >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Eureka Server is healthy
) else (
    echo [WARN] Eureka Server is not responding
)

echo.
echo Checking API Gateway...
curl -s http://localhost:8090 >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] API Gateway is responding
) else (
    echo [WARN] API Gateway is not responding
)

echo.
echo Checking MySQL...
docker-compose exec -T mysql mysqladmin ping -h localhost -uroot -proot >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] MySQL is healthy
) else (
    echo [WARN] MySQL is not responding
)

echo.
echo ========================================
echo Port Usage Check
echo ========================================
echo.
echo Checking if required ports are available...
netstat -ano | findstr ":8761 :8090 :3307 :9092 :2181 :8087 :8082 :8083 :8084 :8085 :8088 :8089 :3000"

echo.
echo ========================================
echo Validation Complete
echo ========================================
echo.
echo For detailed logs: docker-logs.cmd
echo For Eureka dashboard: http://localhost:8761
