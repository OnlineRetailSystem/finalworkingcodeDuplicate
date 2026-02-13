# Docker Setup Guide - E-Commerce Microservices

## Prerequisites

- Docker Desktop installed and running
- At least 8GB RAM allocated to Docker
- 20GB free disk space

## Quick Start

### Option 1: Full Build (First Time)
```cmd
docker-build.cmd
```
This will:
- Clean up any existing containers
- Build all services from scratch
- Start all services
- Takes 10-15 minutes

### Option 2: Start Existing Containers
```cmd
docker-start.cmd
```
Use this if you've already built the images.

## Helper Scripts

| Script | Purpose |
|--------|---------|
| `docker-build.cmd` | Full rebuild and start |
| `docker-start.cmd` | Start existing containers |
| `docker-stop.cmd` | Stop all containers |
| `docker-logs.cmd [service]` | View logs (all or specific service) |
| `docker-clean.cmd` | Remove everything (containers, volumes, images) |

## Service URLs

### User-Facing
- **Frontend**: http://localhost:3000
- **API Gateway**: http://localhost:8090
- **Eureka Dashboard**: http://localhost:8761

### Microservices (Direct Access)
- **Auth Service**: http://localhost:8087
- **Product Service**: http://localhost:8082
- **Order Service**: http://localhost:8083
- **Cart Service**: http://localhost:8084
- **Category Service**: http://localhost:8085
- **Payment Service**: http://localhost:8088
- **Notification Service**: http://localhost:8089

### Infrastructure
- **MySQL**: localhost:3307 (user: root, password: root)
- **Kafka**: localhost:29092
- **Zookeeper**: localhost:2181

## Startup Order

The docker-compose file handles dependencies automatically:

1. **Infrastructure** (MySQL, Zookeeper, Kafka)
2. **Eureka Server** (Service Registry)
3. **API Gateway**
4. **Microservices** (Auth, Product, Order, Cart, Category, Payment, Notification)
5. **Frontend**

## Monitoring

### Check Service Status
```cmd
docker-compose ps
```

### View All Logs
```cmd
docker-compose logs -f
```

### View Specific Service Logs
```cmd
docker-logs.cmd authservice
docker-logs.cmd productservice
docker-logs.cmd mysql
```

### Check Eureka Registration
Visit http://localhost:8761 to see which services are registered.

## Troubleshooting

### Services Not Starting
1. Check Docker Desktop is running
2. Ensure ports are not in use:
   ```cmd
   netstat -ano | findstr "8761 8090 3306 9092"
   ```
3. Check logs:
   ```cmd
   docker-compose logs [service-name]
   ```

### Database Connection Issues
```cmd
docker-compose logs mysql
```
Wait for: "ready for connections" message

### Kafka Issues
```cmd
docker-compose logs kafka
docker-compose logs zookeeper
```

### Service Won't Register with Eureka
1. Check Eureka is healthy: http://localhost:8761
2. Check service logs for connection errors
3. Verify network connectivity:
   ```cmd
   docker-compose exec authservice ping eureka-server
   ```

### Out of Memory
Increase Docker Desktop memory:
- Settings → Resources → Memory → 8GB+

### Rebuild Single Service
```cmd
docker-compose up -d --build authservice
```

## Database Access

### Connect to MySQL
```cmd
docker-compose exec mysql mysql -uroot -proot
```

### View Databases
```sql
SHOW DATABASES;
USE authservice;
SHOW TABLES;
```

### Databases Created
- `authservice` - User authentication
- `productdb` - Product catalog
- `orderdb` - Orders
- `cartdb` - Shopping carts
- `categorydb` - Product categories
- `notificationdb` - Notifications

## Kafka Topics

Auto-created topics:
- `user-events` - User registration/login
- `payment-events` - Payment success
- `order-events` - Order placed/updated
- `inventory-events` - Low stock alerts

### List Topics
```cmd
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Clean Up

### Stop Services (Keep Data)
```cmd
docker-stop.cmd
```

### Remove Containers (Keep Images)
```cmd
docker-compose down
```

### Full Cleanup (Remove Everything)
```cmd
docker-clean.cmd
```

## Performance Tips

1. **First build is slow** - Maven downloads dependencies
2. **Subsequent builds are faster** - Docker layer caching
3. **Use `docker-start.cmd`** after first build
4. **Allocate more RAM** to Docker for better performance

## Architecture Notes

### Image Sizes (Optimized)
- Build stage: ~800MB (maven:3.9-eclipse-temurin-17)
- Runtime stage: ~180MB (eclipse-temurin:17-jre-alpine)
- Final images: ~200-250MB per service

### Network
All services run on `ecom-network` bridge network for inter-service communication.

### Volumes
- `mysql-data` - Persistent MySQL data

## Development Workflow

1. **Initial Setup**
   ```cmd
   docker-build.cmd
   ```

2. **Daily Development**
   ```cmd
   docker-start.cmd
   ```

3. **After Code Changes**
   ```cmd
   docker-compose up -d --build [service-name]
   ```

4. **End of Day**
   ```cmd
   docker-stop.cmd
   ```

## Testing the System

### 1. Register a User
```bash
curl -X POST http://localhost:8090/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"test","password":"test123","email":"test@example.com"}'
```

### 2. Login
```bash
curl -X POST http://localhost:8090/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"test","password":"test123"}'
```

### 3. Check Notifications
```bash
curl http://localhost:8089/notifications/processed-events
```

## Support

For issues:
1. Check logs: `docker-logs.cmd [service]`
2. Verify Eureka: http://localhost:8761
3. Check service health endpoints
4. Review docker-compose.yml configuration
