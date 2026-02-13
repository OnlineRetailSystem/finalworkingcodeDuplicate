# Quick Start Guide - 5 Minutes to Running System

## Prerequisites (2 minutes)

1. **Install Docker Desktop**
   - Download from https://www.docker.com/products/docker-desktop
   - Install and restart computer
   - Start Docker Desktop

2. **Verify Installation**
   ```cmd
   docker --version
   docker-compose --version
   ```

## Start the System (3 minutes)

### Step 1: Navigate to Project
```cmd
cd "Ecom-Backend-Final 4\Ecom-Backend-Final"
```

### Step 2: Build and Start (First Time)
```cmd
docker-build.cmd
```

**Wait 10-15 minutes** for first build (downloads dependencies)

### Step 3: Verify Services
```cmd
docker-validate.cmd
```

## Access the System

### Main URLs
- **Frontend:** http://localhost:3000
- **API Gateway:** http://localhost:8090
- **Eureka Dashboard:** http://localhost:8761

### Test the API
```cmd
REM Register a user
curl -X POST http://localhost:8090/auth/register ^
  -H "Content-Type: application/json" ^
  -d "{\"username\":\"demo\",\"password\":\"demo123\",\"email\":\"demo@example.com\"}"

REM Login
curl -X POST http://localhost:8090/auth/login ^
  -H "Content-Type: application/json" ^
  -d "{\"username\":\"demo\",\"password\":\"demo123\"}"
```

## Daily Usage

### Start Services
```cmd
docker-start.cmd
```

### Stop Services
```cmd
docker-stop.cmd
```

### View Logs
```cmd
docker-logs.cmd
```

## Troubleshooting

### Services Not Starting?
```cmd
REM Check what's running
docker-compose ps

REM View logs
docker-compose logs -f

REM Restart everything
docker-compose restart
```

### Port Conflicts?
```cmd
REM Find what's using the port
netstat -ano | findstr :8761

REM Kill the process (replace PID)
taskkill /PID <PID> /F
```

### Need Fresh Start?
```cmd
docker-clean.cmd
docker-build.cmd
```

## What's Running?

| Service | Port | Purpose |
|---------|------|---------|
| Frontend | 3000 | React UI |
| API Gateway | 8090 | Entry point |
| Eureka | 8761 | Service registry |
| Auth | 8087 | Authentication |
| Product | 8082 | Products |
| Order | 8083 | Orders |
| Cart | 8084 | Shopping cart |
| Category | 8085 | Categories |
| Payment | 8088 | Payments |
| Notification | 8089 | Notifications |
| MySQL | 3307 | Database |
| Kafka | 29092 | Events |

## Next Steps

1. **Read Full Documentation**
   - DOCKER-SETUP.md - Complete setup guide
   - TROUBLESHOOTING.md - Common issues
   - DEPLOYMENT-CHECKLIST.md - Deployment steps

2. **Explore the System**
   - Check Eureka: http://localhost:8761
   - Test APIs via API Gateway
   - Monitor logs: `docker-logs.cmd`

3. **Develop**
   - Make code changes
   - Rebuild specific service: `docker-compose up -d --build [service]`
   - Test changes

## Common Commands

```cmd
REM Build and start everything
docker-build.cmd

REM Start existing containers
docker-start.cmd

REM Stop containers
docker-stop.cmd

REM View all logs
docker-logs.cmd

REM View specific service logs
docker-logs.cmd authservice

REM Check health
docker-validate.cmd

REM Check status
docker-compose ps

REM Clean everything
docker-clean.cmd
```

## Success Checklist

- [ ] Docker Desktop running
- [ ] All services built successfully
- [ ] All containers showing "Up" or "Up (healthy)"
- [ ] Eureka dashboard accessible
- [ ] Services registered in Eureka
- [ ] API endpoints responding
- [ ] No errors in logs

## Getting Help

1. **Check logs first:** `docker-logs.cmd [service]`
2. **Validate setup:** `docker-validate.cmd`
3. **Read troubleshooting:** TROUBLESHOOTING.md
4. **Check Eureka:** http://localhost:8761

## Tips

- **First build is slow** - Maven downloads dependencies
- **Subsequent starts are fast** - Use `docker-start.cmd`
- **Watch logs during startup** - `docker-compose logs -f`
- **Wait for health checks** - Services need 2-3 minutes to fully start
- **Check Eureka** - All services should appear there

## That's It!

You now have a fully functional microservices e-commerce platform running locally.

**Happy coding! 🚀**
