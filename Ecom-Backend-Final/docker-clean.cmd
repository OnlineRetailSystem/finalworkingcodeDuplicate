@echo off
REM Clean up all containers, volumes, and images

echo ========================================
echo WARNING: This will remove all containers,
echo volumes, and images for this project!
echo ========================================
echo.
set /p confirm="Are you sure? (yes/no): "

if /i "%confirm%"=="yes" (
    echo.
    echo Stopping and removing containers...
    docker-compose down -v
    
    echo.
    echo Removing images...
    docker-compose down --rmi all
    
    echo.
    echo Cleanup complete!
) else (
    echo Cleanup cancelled.
)
