@echo off
REM Stop all containers

echo Stopping all services...
docker-compose stop

echo.
echo All services stopped.
echo To start again: docker-start.cmd
echo To remove containers: docker-compose down
