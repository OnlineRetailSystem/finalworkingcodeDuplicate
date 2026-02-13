@echo off
REM Build and start all services with Docker Compose

echo ========================================
echo Building E-Commerce Microservices
echo ========================================

echo.
echo Step 1: Cleaning up old containers and images...
docker-compose down -v

echo.
echo Step 2: Building all services (this may take 10-15 minutes)...
docker-compose build --no-cache

echo.
echo Step 3: Starting all services...
docker-compose up -d

echo.
echo ========================================
echo Build Complete!
echo ========================================
echo.
echo Services are starting up. This may take 2-3 minutes.
echo.
echo Monitor logs with: docker-compose logs -f
echo Check status with: docker-compose ps
echo.
echo Service URLs:
echo - Eureka Dashboard: http://localhost:8761
echo - API Gateway: http://localhost:8090
echo - Frontend: http://localhost:3000
echo.
echo Individual Services:
echo - Auth Service: http://localhost:8087
echo - Product Service: http://localhost:8082
echo - Order Service: http://localhost:8083
echo - Cart Service: http://localhost:8084
echo - Category Service: http://localhost:8085
echo - Payment Service: http://localhost:8088
echo - Notification Service: http://localhost:8089
echo.
echo Infrastructure:
echo - MySQL: localhost:3307
echo - Kafka: localhost:29092
echo - Zookeeper: localhost:2181
echo ========================================
