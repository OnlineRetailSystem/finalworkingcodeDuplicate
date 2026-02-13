# Deployment Checklist - Local Docker Setup

## Pre-Deployment Checks

### System Requirements
- [ ] Docker Desktop installed (version 20.10+)
- [ ] Docker Compose installed (version 2.0+)
- [ ] 8GB+ RAM allocated to Docker
- [ ] 20GB+ free disk space
- [ ] Windows 10/11 with WSL2 enabled (for Windows)

### Port Availability
Check these ports are free:
- [ ] 8761 (Eureka Server)
- [ ] 8090 (API Gateway)
- [ ] 8087 (Auth Service)
- [ ] 8082 (Product Service)
- [ ] 8083 (Order Service)
- [ ] 8084 (Cart Service)
- [ ] 8085 (Category Service)
- [ ] 8088 (Payment Service)
- [ ] 8089 (Notification Service)
- [ ] 3000 (Frontend)
- [ ] 3307 (MySQL)
- [ ] 9092, 29092 (Kafka)
- [ ] 2181 (Zookeeper)

Check with:
```cmd
netstat -ano | findstr ":8761 :8090 :3307 :9092"
```

## Deployment Steps

### 1. Initial Build
```cmd
cd "Ecom-Backend-Final 4/Ecom-Backend-Final"
docker-build.cmd
```

**Expected time:** 10-15 minutes

**What happens:**
- Cleans up old containers
- Builds all Docker images
- Starts all services
- Initializes databases

### 2. Monitor Startup
```cmd
docker-compose logs -f
```

**Wait for these messages:**
- MySQL: `ready for connections`
- Kafka: `[KafkaServer id=1] started`
- Eureka: `Started EurekaServerApplication`
- Services: `Started [ServiceName]Application`

**Expected time:** 2-3 minutes after build

### 3. Validate Deployment
```cmd
docker-validate.cmd
```

**Check:**
- [ ] All containers are "Up" or "Up (healthy)"
- [ ] Eureka Server responds at http://localhost:8761
- [ ] API Gateway responds at http://localhost:8090
- [ ] MySQL is healthy

### 4. Verify Service Registration
Open http://localhost:8761

**Should see registered:**
- [ ] APIGATEWAY
- [ ] AUTHSERVICE
- [ ] PRODUCTSERVICE
- [ ] ORDERSERVICE
- [ ] CARTSERVICE
- [ ] CATEGORYSERVICE
- [ ] NOTIFICATIONSERVICE

**Note:** Payment service doesn't register with Eureka (by design)

### 5. Test Database Connectivity
```cmd
docker-compose exec mysql mysql -uroot -proot -e "SHOW DATABASES;"
```

**Should see:**
- [ ] authservice
- [ ] productdb
- [ ] orderdb
- [ ] cartdb
- [ ] categorydb
- [ ] notificationdb

### 6. Test Kafka
```cmd
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Topics will be auto-created on first use**

### 7. Test API Endpoints

#### Health Check
```cmd
curl http://localhost:8761/actuator/health
```
Expected: `{"status":"UP"}`

#### Register User
```cmd
curl -X POST http://localhost:8090/auth/register ^
  -H "Content-Type: application/json" ^
  -d "{\"username\":\"testuser\",\"password\":\"test123\",\"email\":\"test@example.com\"}"
```

#### Login
```cmd
curl -X POST http://localhost:8090/auth/login ^
  -H "Content-Type: application/json" ^
  -d "{\"username\":\"testuser\",\"password\":\"test123\"}"
```

### 8. Verify Event Flow

After registration, check notification service:
```cmd
curl http://localhost:8089/notifications/processed-events
```

Should show USER_REGISTERED event was processed.

## Post-Deployment

### Daily Operations

#### Start Services
```cmd
docker-start.cmd
```

#### Stop Services
```cmd
docker-stop.cmd
```

#### View Logs
```cmd
docker-logs.cmd [service-name]
```

#### Check Status
```cmd
docker-compose ps
```

### Monitoring

#### Service Health
- Eureka Dashboard: http://localhost:8761
- Check all services are registered
- Green status indicators

#### Resource Usage
```cmd
docker stats
```

Monitor:
- CPU usage (should be < 50% per service)
- Memory usage (should be < 500MB per service)
- Network I/O

#### Logs
```cmd
docker-compose logs -f --tail=100
```

Watch for:
- ERROR messages
- Connection failures
- Timeout exceptions

## Troubleshooting

### If Services Don't Start
1. Check logs: `docker-compose logs [service]`
2. Verify dependencies: `docker-compose ps`
3. Check ports: `netstat -ano | findstr :[port]`
4. See TROUBLESHOOTING.md

### If Services Don't Register with Eureka
1. Wait 30-60 seconds (registration takes time)
2. Check Eureka logs: `docker-compose logs eureka-server`
3. Check service logs for connection errors
4. Verify network: `docker network inspect ecom-backend-final_ecom-network`

### If Database Connection Fails
1. Check MySQL is healthy: `docker-compose ps mysql`
2. Wait for "ready for connections" in logs
3. Verify databases exist: `docker-compose exec mysql mysql -uroot -proot -e "SHOW DATABASES;"`
4. Restart service: `docker-compose restart [service]`

## Rollback Procedure

### If Deployment Fails
```cmd
REM Stop all services
docker-compose down

REM Remove volumes (if needed)
docker-compose down -v

REM Rebuild
docker-build.cmd
```

### If Data Corruption
```cmd
REM Backup first (if possible)
docker-compose exec mysql mysqldump -uroot -proot --all-databases > backup.sql

REM Remove volumes and restart
docker-compose down -v
docker-compose up -d
```

## Maintenance

### Weekly
- [ ] Check disk space: `docker system df`
- [ ] Review logs for errors
- [ ] Verify all services are healthy

### Monthly
- [ ] Clean unused images: `docker image prune -a`
- [ ] Clean unused volumes: `docker volume prune`
- [ ] Update Docker Desktop
- [ ] Backup database

### Backup Database
```cmd
docker-compose exec mysql mysqldump -uroot -proot --all-databases > backup_%date:~-4,4%%date:~-10,2%%date:~-7,2%.sql
```

### Restore Database
```cmd
docker-compose exec -T mysql mysql -uroot -proot < backup.sql
```

## Performance Optimization

### If Services Are Slow
1. Increase Docker resources
   - Settings → Resources → Memory (8GB+)
   - Settings → Resources → CPUs (4+)

2. Add JVM tuning in Dockerfiles:
   ```dockerfile
   ENTRYPOINT ["java", "-Xmx512m", "-Xms256m", "-XX:+UseG1GC", "-jar", "app.jar"]
   ```

3. Use SSD for Docker storage

### If Build Is Slow
1. Don't use `--no-cache` unless necessary
2. Increase Docker CPU allocation
3. Check internet connection (Maven downloads)

## Security Notes

### For Production
⚠️ This setup is for LOCAL DEVELOPMENT ONLY

**Before production:**
- [ ] Change default passwords
- [ ] Use secrets management
- [ ] Enable SSL/TLS
- [ ] Configure firewalls
- [ ] Use environment-specific configs
- [ ] Enable authentication on Kafka
- [ ] Secure Eureka dashboard
- [ ] Add API rate limiting
- [ ] Enable audit logging

## Success Criteria

Deployment is successful when:
- [ ] All containers are running
- [ ] All services registered with Eureka
- [ ] Database connections working
- [ ] Kafka topics created
- [ ] API endpoints responding
- [ ] Event flow working (registration → notification)
- [ ] No ERROR logs in services
- [ ] Frontend accessible at http://localhost:3000

## Quick Reference

| Command | Purpose |
|---------|---------|
| `docker-build.cmd` | Full rebuild and start |
| `docker-start.cmd` | Start existing containers |
| `docker-stop.cmd` | Stop all containers |
| `docker-logs.cmd [service]` | View logs |
| `docker-validate.cmd` | Check health |
| `docker-clean.cmd` | Remove everything |
| `docker-compose ps` | Check status |
| `docker stats` | Resource usage |

## Support Resources

- **Setup Guide:** DOCKER-SETUP.md
- **Troubleshooting:** TROUBLESHOOTING.md
- **Architecture:** README.md
- **Eureka Dashboard:** http://localhost:8761
- **API Gateway:** http://localhost:8090

## Sign-Off

- [ ] All pre-deployment checks passed
- [ ] Build completed successfully
- [ ] All services started
- [ ] Service registration verified
- [ ] Database connectivity confirmed
- [ ] API endpoints tested
- [ ] Event flow verified
- [ ] No critical errors in logs
- [ ] Documentation reviewed
- [ ] Team notified

**Deployed by:** _______________  
**Date:** _______________  
**Time:** _______________  
**Version:** _______________
