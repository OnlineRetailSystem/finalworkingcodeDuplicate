# Ecom Backend - Event Driven Microservices

This project is a refactored version of the Ecom Backend, transformed into an Event-Driven Architecture using Apache Kafka. It includes microservices for Auth, Product, Order, Payment, Category, Cart, and Notifications.

## 🚀 Quick Start

**New to this project? Start here:**

1. **[QUICK-START.md](QUICK-START.md)** - Get running in 5 minutes
2. **[DOCKER-SETUP.md](DOCKER-SETUP.md)** - Complete setup guide
3. **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common issues and solutions

### One Command Setup
```cmd
docker-build.cmd
```

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| [QUICK-START.md](QUICK-START.md) | 5-minute quick start guide |
| [DOCKER-SETUP.md](DOCKER-SETUP.md) | Complete Docker setup and usage |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Common issues and solutions |
| [DEPLOYMENT-CHECKLIST.md](DEPLOYMENT-CHECKLIST.md) | Step-by-step deployment guide |
| [CHANGES-SUMMARY.md](CHANGES-SUMMARY.md) | Recent improvements and changes |

## Architecture Overview

The system uses the following event flows:

1.  **User Registration / Login**:
    *   **Auth Service** publishes `USER_REGISTERED` / `USER_LOGGED_IN`.
    *   **Notification Service** consumes these events and sends welcome/login alerts.

2.  **Order Processing**:
    *   **Payment Service** publishes `PAYMENT_SUCCESS`.
    *   **Order Service** consumes `PAYMENT_SUCCESS`, creates an order (Idempotent), and publishes `ORDER_PLACED`.
    *   **Product Service** consumes `ORDER_PLACED`, reduces stock.
        *   If stock < threshold -> publishes `LOW_STOCK_ALERT`.
    *   **Notification Service** consumes `LOW_STOCK_ALERT` -> notifies admin.

3.  **Shipping Updates**:
    *   **Order Service** (Admin) updates shipping status via REST (`PUT /orders/{id}/shipping-status`).
    *   **Order Service** publishes `ORDER_STATUS_UPDATED`.
    *   **Notification Service** consumes this and notifies the user.

## Services

| Service | Port | Description |
|---------|------|-------------|
| Eureka Server | 8761 | Service registry and discovery |
| API Gateway | 8090 | Single entry point for all services |
| Auth Service | 8087 | User authentication and authorization |
| Product Service | 8082 | Product catalog and inventory |
| Order Service | 8083 | Order management |
| Cart Service | 8084 | Shopping cart |
| Category Service | 8085 | Product categories |
| Payment Service | 8088 | Payment processing (Stripe) |
| Notification Service | 8089 | Event-driven notifications |
| Frontend | 3000 | React web application |

## Infrastructure

| Component | Port | Purpose |
|-----------|------|---------|
| MySQL | 3307 | Relational database |
| Kafka | 29092 | Event streaming |
| Zookeeper | 2181 | Kafka coordination |

## Prerequisites

*   Docker & Docker Compose
*   8GB+ RAM allocated to Docker
*   20GB+ free disk space

## Running the Application

### First Time Setup
```cmd
docker-build.cmd
```

### Daily Usage
```cmd
REM Start services
docker-start.cmd

REM Stop services
docker-stop.cmd

REM View logs
docker-logs.cmd [service-name]

REM Check health
docker-validate.cmd
```

## Verify Services

*   Eureka Dashboard: [http://localhost:8761](http://localhost:8761)
*   API Gateway: [http://localhost:8090](http://localhost:8090)
*   Frontend: [http://localhost:3000](http://localhost:3000)

## API Endpoints

### Auth Service (Port 8087)
*   `POST /auth/register` - Register user (Triggers `USER_REGISTERED`)
*   `POST /auth/login` - Login (Triggers `USER_LOGGED_IN`)

### Payment Service (Port 8088)
*   `POST /payments/charge` - Charge card (Triggers `PAYMENT_SUCCESS`)
    *   Body: `{ "amount": 100, "currency": "usd", "order_details": { ... } }`

### Order Service (Port 8083)
*   `GET /orders` - List orders
*   `PUT /orders/{id}/shipping-status?status=SHIPPED` - Update status (Triggers `ORDER_STATUS_UPDATED`)

### Notification Service (Port 8089)
*   `GET /notifications/processed-events` - View processed events (Idempotency check)

## Idempotency & Error Handling
*   **Idempotency**: Implemented using unique `eventId` in Kafka messages. Consumers track processed IDs in a database to prevent duplicate processing.
*   **Error Handling**: Kafka consumers use `ErrorHandlingDeserializer` to manage serialization issues gracefully.

## Development

### Rebuild Single Service
```cmd
docker-compose up -d --build [service-name]
```

### Access Database
```cmd
docker-compose exec mysql mysql -uroot -proot
```

### View Kafka Topics
```cmd
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

## Troubleshooting

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed solutions.

**Quick fixes:**
```cmd
REM Check status
docker-compose ps

REM View logs
docker-compose logs -f [service]

REM Restart service
docker-compose restart [service]

REM Clean slate
docker-clean.cmd
docker-build.cmd
```

## Recent Improvements

### Backend Services
✅ Optimized Docker images (75% smaller)  
✅ Fixed health checks and startup order  
✅ Added helper scripts for easy management  
✅ Comprehensive documentation  
✅ Better security (removed SSL bypass flags)

### Frontend
✅ Optimized Dockerfile with health checks  
✅ Enhanced Nginx configuration (gzip, caching, security headers)  
✅ Centralized API configuration system  
✅ Environment-specific settings (.env files)  
✅ Comprehensive documentation

See [CHANGES-SUMMARY.md](CHANGES-SUMMARY.md) for backend details.  
See [frontend/CHANGES-SUMMARY.md](../frontend/CHANGES-SUMMARY.md) for frontend details.

## Project Structure

```
Ecom-Backend-Final/
├── apigateway/          # API Gateway service
├── authservice/         # Authentication service
├── cartservice/         # Shopping cart service
├── categoryservice/     # Category management
├── eureka-server/       # Service registry
├── notificationservice/ # Notification service
├── orderservice/        # Order management
├── paymentservice/      # Payment processing
├── productservice/      # Product catalog
├── docker-compose.yml   # Docker orchestration
├── init-databases.sql   # Database initialization
└── *.cmd               # Helper scripts
```

## Technology Stack

- **Backend:** Spring Boot 3.5.5, Java 17
- **Service Discovery:** Netflix Eureka
- **API Gateway:** Spring Cloud Gateway
- **Database:** MySQL 8.0
- **Event Streaming:** Apache Kafka 7.5.0
- **Containerization:** Docker & Docker Compose
- **Frontend:** React (separate repository)

## Contributing

1. Make changes to code
2. Rebuild affected service: `docker-compose up -d --build [service]`
3. Test changes
4. Check logs: `docker-logs.cmd [service]`

## License

[Your License Here]

## Support

- **Documentation:** See docs listed above
- **Issues:** Check TROUBLESHOOTING.md first
- **Validation:** Run `docker-validate.cmd`
