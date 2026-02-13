# Troubleshooting Guide

## Common Issues and Solutions

### 1. Services Won't Start

#### Symptom
```
ERROR: Service 'authservice' failed to build
```

#### Solutions
1. **Check Docker is running**
   ```cmd
   docker ps
   ```

2. **Clean and rebuild**
   ```cmd
   docker-compose down -v
   docker-compose build --no-cache
   docker-compose up -d
   ```

3. **Check disk space**
   ```cmd
   docker system df
   ```
   If low, clean up:
   ```cmd
   docker system prune -a
   ```

### 2. Port Already in Use

#### Symptom
```
ERROR: for mysql  Cannot start service mysql: Ports are not available
```

#### Solutions
1. **Find what's using the port**
   ```cmd
   netstat -ano | findstr :3307
   ```

2. **Kill the process** (replace PID with actual process ID)
   ```cmd
   taskkill /PID <PID> /F
   ```

3. **Or change the port** in docker-compose.yml:
   ```yaml
   ports:
     - "3308:3306"  # Changed from 3307
   ```

### 3. MySQL Connection Refused

#### Symptom
```
java.sql.SQLNonTransientConnectionException: Could not connect to address=(host=mysql)(port=3306)
```

#### Solutions
1. **Wait for MySQL to be ready**
   ```cmd
   docker-compose logs mysql
   ```
   Look for: `ready for connections`

2. **Check MySQL health**
   ```cmd
   docker-compose exec mysql mysqladmin ping -h localhost -uroot -proot
   ```

3. **Restart MySQL**
   ```cmd
   docker-compose restart mysql
   ```

4. **Check databases were created**
   ```cmd
   docker-compose exec mysql mysql -uroot -proot -e "SHOW DATABASES;"
   ```

### 4. Kafka Connection Issues

#### Symptom
```
org.apache.kafka.common.errors.TimeoutException: Failed to update metadata
```

#### Solutions
1. **Check Kafka is running**
   ```cmd
   docker-compose logs kafka
   ```

2. **Verify Zookeeper is healthy**
   ```cmd
   docker-compose logs zookeeper
   ```

3. **Restart Kafka stack**
   ```cmd
   docker-compose restart zookeeper
   timeout /t 10
   docker-compose restart kafka
   ```

4. **List topics to verify connectivity**
   ```cmd
   docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

### 5. Eureka Registration Failures

#### Symptom
Services not appearing in Eureka dashboard (http://localhost:8761)

#### Solutions
1. **Check Eureka is healthy**
   ```cmd
   curl http://localhost:8761/actuator/health
   ```

2. **Check service logs**
   ```cmd
   docker-compose logs authservice | findstr "eureka"
   ```

3. **Verify network connectivity**
   ```cmd
   docker-compose exec authservice ping eureka-server
   ```

4. **Check application.properties**
   Ensure:
   ```properties
   eureka.client.service-url.defaultZone=http://eureka-server:8761/eureka
   ```

5. **Restart the service**
   ```cmd
   docker-compose restart authservice
   ```

### 6. Service Crashes on Startup

#### Symptom
```
Container exits with code 1
```

#### Solutions
1. **Check logs for stack trace**
   ```cmd
   docker-compose logs authservice
   ```

2. **Common causes:**
   - Missing environment variables
   - Database not ready
   - Port conflicts
   - Dependency issues

3. **Verify dependencies are healthy**
   ```cmd
   docker-compose ps
   ```
   All should show "Up" or "Up (healthy)"

4. **Check for Java errors**
   Look for:
   - `ClassNotFoundException`
   - `NoSuchMethodError`
   - `OutOfMemoryError`

### 7. Out of Memory

#### Symptom
```
java.lang.OutOfMemoryError: Java heap space
```

#### Solutions
1. **Increase Docker memory**
   - Docker Desktop → Settings → Resources
   - Set Memory to 8GB or more

2. **Add JVM options** in Dockerfile:
   ```dockerfile
   ENTRYPOINT ["java", "-Xmx512m", "-Xms256m", "-jar", "app.jar"]
   ```

3. **Reduce number of running services**
   ```cmd
   docker-compose up -d mysql kafka zookeeper eureka-server authservice
   ```

### 8. Build Takes Too Long

#### Symptom
Maven build hangs or takes 30+ minutes

#### Solutions
1. **Check internet connection**
   Maven needs to download dependencies

2. **Use Docker build cache**
   Don't use `--no-cache` unless necessary

3. **Pre-download dependencies**
   ```cmd
   cd authservice
   mvnw dependency:go-offline
   ```

4. **Increase Docker CPU allocation**
   Docker Desktop → Settings → Resources → CPUs

### 9. Frontend Can't Connect to Backend

#### Symptom
```
Network Error: Failed to fetch
```

#### Solutions
1. **Check API Gateway is running**
   ```cmd
   curl http://localhost:8090
   ```

2. **Verify CORS configuration**
   Check API Gateway allows frontend origin

3. **Check browser console**
   Look for CORS or network errors

4. **Test backend directly**
   ```cmd
   curl http://localhost:8087/auth/health
   ```

### 10. Database Data Lost After Restart

#### Symptom
All data disappears when containers restart

#### Solutions
1. **Use named volumes** (already configured):
   ```yaml
   volumes:
     - mysql-data:/var/lib/mysql
   ```

2. **Don't use `-v` flag** when stopping:
   ```cmd
   docker-compose down     # Keeps volumes
   docker-compose down -v  # DELETES volumes
   ```

3. **Backup database**
   ```cmd
   docker-compose exec mysql mysqldump -uroot -proot --all-databases > backup.sql
   ```

## Diagnostic Commands

### Check All Service Status
```cmd
docker-compose ps
```

### View All Logs
```cmd
docker-compose logs -f
```

### View Specific Service Logs
```cmd
docker-compose logs -f authservice
```

### Check Service Health
```cmd
docker-compose exec authservice curl http://localhost:8087/actuator/health
```

### Inspect Container
```cmd
docker-compose exec authservice sh
```

### Check Network
```cmd
docker network inspect ecom-backend-final_ecom-network
```

### Check Volumes
```cmd
docker volume ls
docker volume inspect ecom-backend-final_mysql-data
```

### Resource Usage
```cmd
docker stats
```

## Performance Issues

### Slow Startup
1. Increase Docker resources (CPU, Memory)
2. Use SSD for Docker storage
3. Disable antivirus scanning of Docker directories

### High CPU Usage
1. Check for infinite loops in logs
2. Reduce number of running services
3. Check Kafka consumer lag

### High Memory Usage
1. Add JVM memory limits
2. Reduce Docker memory allocation per service
3. Monitor with `docker stats`

## Clean Slate Approach

If nothing works, start fresh:

```cmd
REM 1. Stop everything
docker-compose down -v

REM 2. Remove all images
docker-compose down --rmi all

REM 3. Clean Docker system
docker system prune -a --volumes

REM 4. Rebuild from scratch
docker-compose build --no-cache

REM 5. Start services
docker-compose up -d
```

## Getting Help

1. **Check logs first**
   ```cmd
   docker-compose logs [service-name]
   ```

2. **Validate setup**
   ```cmd
   docker-validate.cmd
   ```

3. **Check Eureka dashboard**
   http://localhost:8761

4. **Test connectivity**
   ```cmd
   docker-compose exec authservice ping mysql
   docker-compose exec authservice ping kafka
   ```

## Prevention Tips

1. **Always wait for health checks**
   Don't test services immediately after `docker-compose up`

2. **Monitor logs during startup**
   ```cmd
   docker-compose up -d && docker-compose logs -f
   ```

3. **Use validation script**
   ```cmd
   docker-validate.cmd
   ```

4. **Regular cleanup**
   ```cmd
   docker system prune
   ```

5. **Keep Docker Desktop updated**
   Check for updates regularly
