@echo off
REM Start existing containers

echo Starting all services...
docker-compose up -d

echo.
echo Services started!
echo Monitor with: docker-compose logs -f
echo Check status: docker-compose ps
