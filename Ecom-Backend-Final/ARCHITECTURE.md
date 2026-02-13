# System Architecture

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         FRONTEND (React)                         │
│                        http://localhost:3000                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      API GATEWAY (8090)                          │
│                   Spring Cloud Gateway                           │
│              Routes requests to microservices                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   EUREKA SERVER (8761)                           │
│                   Service Registry                               │
│         All microservices register here                          │
└─────────────────────────────────────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Auth Service │    │Product Service│   │ Order Service│
│   (8087)     │    │    (8082)     │   │   (8083)     │
└──────┬───────┘    └──────┬────────┘   └──────┬───────┘
       │                   │                    │
       │                   │                    │
        ▼                  ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Cart Service │    │Category Svc  │   │Notification  │
│   (8084)     │    │    (8085)     │   │Service(8089) │
└──────────────┘    └───────────────┘   └──────┬───────┘
                                               │
                    ┌──────────────┐           │
                    │Payment Service│          │
                    │    (8088)     │          │
                    └──────┬────────┘          │
                           │                   │
        ┌──────────────────┴───────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                      KAFKA (9092)                                │
│                   Event Streaming Platform                       │
│   Topics: user-events, payment-events, order-events, etc.       │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ZOOKEEPER (2181)                               │
│                Kafka Coordination Service                        │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      MySQL (3307)                                │
│  Databases: authservice, productdb, orderdb, cartdb,            │
│             categorydb, notificationdb                           │
└─────────────────────────────────────────────────────────────────┘
```

## Service Communication Patterns

### Synchronous (REST via API Gateway)
```
Frontend → API Gateway → Microservice → Database
```

### Asynchronous (Event-Driven via Kafka)
```
Service A → Kafka Topic → Service B
```

## Event Flow Diagrams

### 1. User Registration Flow
```
┌──────────┐     ┌──────────┐     ┌───────┐     ┌──────────────┐
│ Frontend │────▶│   API    │────▶│ Auth  │────▶│   MySQL      │
│          │     │ Gateway  │     │Service│     │(authservice) │
└──────────┘     └──────────┘     └───┬───┘     └──────────────┘
                                       │
                                       │ Publish USER_REGISTERED
                                       ▼
                                  ┌────────┐
                                  │ Kafka  │
                                  │ Topic  │
                                  └───┬────┘
                                      │
                                      │ Consume
                                      ▼
                              ┌──────────────┐
                              │Notification  │
                              │   Service    │
                              └──────┬───────┘
                                     │
                                     ▼
                              Send Welcome Email
```

### 2. Order Processing Flow
```
┌──────────┐     ┌──────────┐     ┌─────────┐
│ Frontend │────▶│   API    │────▶│ Payment │
│          │     │ Gateway  │     │ Service │
└──────────┘     └──────────┘     └────┬────┘
                                        │
                                        │ Publish PAYMENT_SUCCESS
                                        ▼
                                   ┌────────┐
                                   │ Kafka  │
                                   └───┬────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
                    ▼                  ▼                  ▼
            ┌──────────────┐   ┌──────────────┐  ┌──────────────┐
            │    Order     │   │   Product    │  │Notification  │
            │   Service    │   │   Service    │  │   Service    │
            └──────┬───────┘   └──────┬───────┘  └──────────────┘
                   │                  │
                   │ Create Order     │ Reduce Stock
                   │ (Idempotent)     │
                   ▼                  ▼
            ┌──────────────┐   ┌──────────────┐
            │    MySQL     │   │    MySQL     │
            │  (orderdb)   │   │ (productdb)  │
            └──────────────┘   └──────────────┘
                   │
                   │ Publish ORDER_PLACED
                   ▼
              ┌────────┐
              │ Kafka  │
              └────────┘
```

### 3. Low Stock Alert Flow
```
┌──────────────┐
│   Product    │
│   Service    │
└──────┬───────┘
       │
       │ Check Stock < Threshold
       │
       │ Publish LOW_STOCK_ALERT
       ▼
  ┌────────┐
  │ Kafka  │
  └───┬────┘
      │
      │ Consume
      ▼
┌──────────────┐
│Notification  │
│   Service    │
└──────┬───────┘
       │
       ▼
  Notify Admin
```

## Database Schema

### Auth Service Database (authservice)
```
users
├── id (PK)
├── username
├── password (hashed)
├── email
├── created_at
└── updated_at
```

### Product Service Database (productdb)
```
products
├── id (PK)
├── name
├── description
├── price
├── stock_quantity
├── category_id (FK)
├── created_at
└── updated_at
```

### Order Service Database (orderdb)
```
orders
├── id (PK)
├── user_id
├── total_amount
├── status
├── shipping_status
├── created_at
└── updated_at

order_items
├── id (PK)
├── order_id (FK)
├── product_id
├── quantity
├── price
└── subtotal
```

### Cart Service Database (cartdb)
```
carts
├── id (PK)
├── user_id
├── created_at
└── updated_at

cart_items
├── id (PK)
├── cart_id (FK)
├── product_id
├── quantity
└── added_at
```

### Category Service Database (categorydb)
```
categories
├── id (PK)
├── name
├── description
├── parent_id (FK, nullable)
├── created_at
└── updated_at
```

### Notification Service Database (notificationdb)
```
notifications
├── id (PK)
├── user_id
├── type
├── message
├── read
├── created_at
└── updated_at

processed_events
├── id (PK)
├── event_id (UNIQUE)
├── event_type
├── processed_at
└── status
```

## Kafka Topics

### user-events
```
Events:
- USER_REGISTERED
- USER_LOGGED_IN
- USER_UPDATED

Producers: Auth Service
Consumers: Notification Service
```

### payment-events
```
Events:
- PAYMENT_SUCCESS
- PAYMENT_FAILED

Producers: Payment Service
Consumers: Order Service, Notification Service
```

### order-events
```
Events:
- ORDER_PLACED
- ORDER_STATUS_UPDATED
- ORDER_CANCELLED

Producers: Order Service
Consumers: Product Service, Notification Service
```

### inventory-events
```
Events:
- LOW_STOCK_ALERT
- STOCK_UPDATED

Producers: Product Service
Consumers: Notification Service
```

## Service Dependencies

### Auth Service
- **Depends on:** MySQL, Kafka, Eureka
- **Used by:** All services (via API Gateway)
- **Publishes:** user-events
- **Consumes:** None

### Product Service
- **Depends on:** MySQL, Kafka, Eureka
- **Used by:** Order Service, Cart Service
- **Publishes:** inventory-events
- **Consumes:** order-events

### Order Service
- **Depends on:** MySQL, Kafka, Eureka
- **Used by:** Frontend (via API Gateway)
- **Publishes:** order-events
- **Consumes:** payment-events

### Payment Service
- **Depends on:** Kafka
- **Used by:** Frontend (via API Gateway)
- **Publishes:** payment-events
- **Consumes:** None

### Cart Service
- **Depends on:** MySQL, Eureka
- **Used by:** Frontend (via API Gateway)
- **Publishes:** None
- **Consumes:** None

### Category Service
- **Depends on:** MySQL, Eureka
- **Used by:** Product Service, Frontend
- **Publishes:** None
- **Consumes:** None

### Notification Service
- **Depends on:** MySQL, Kafka, Eureka
- **Used by:** None (background service)
- **Publishes:** None
- **Consumes:** user-events, payment-events, order-events, inventory-events

## Network Architecture

### Docker Network: ecom-network
```
Bridge Network (172.x.x.x)
├── mysql (hostname: mysql)
├── zookeeper (hostname: zookeeper)
├── kafka (hostname: kafka)
├── eureka-server (hostname: eureka-server)
├── apigateway (hostname: apigateway)
├── authservice (hostname: authservice)
├── productservice (hostname: productservice)
├── orderservice (hostname: orderservice)
├── cartservice (hostname: cartservice)
├── categoryservice (hostname: categoryservice)
├── paymentservice (hostname: paymentservice)
├── notificationservice (hostname: notificationservice)
└── frontend (hostname: frontend)
```

### Port Mapping (Host:Container)
```
3000:80    → Frontend
8090:8090  → API Gateway
8761:8761  → Eureka Server
8087:8087  → Auth Service
8082:8082  → Product Service
8083:8083  → Order Service
8084:8084  → Cart Service
8085:8085  → Category Service
8088:8088  → Payment Service
8089:8089  → Notification Service
3307:3306  → MySQL
9092:9092  → Kafka (internal)
29092:29092→ Kafka (external)
2181:2181  → Zookeeper
```

## Startup Sequence

```
1. Infrastructure Layer
   ├── MySQL (with health check)
   ├── Zookeeper (with health check)
   └── Kafka (depends on Zookeeper, with health check)

2. Service Registry
   └── Eureka Server (with health check)

3. API Gateway
   └── API Gateway (depends on Eureka)

4. Business Services (all depend on Eureka + their databases)
   ├── Auth Service (MySQL + Kafka)
   ├── Product Service (MySQL + Kafka)
   ├── Order Service (MySQL + Kafka)
   ├── Cart Service (MySQL)
   ├── Category Service (MySQL)
   ├── Payment Service (Kafka)
   └── Notification Service (MySQL + Kafka)

5. Frontend
   └── React App (depends on API Gateway)
```

## Scalability Considerations

### Horizontal Scaling
```
Load Balancer
      │
      ├─▶ API Gateway Instance 1
      ├─▶ API Gateway Instance 2
      └─▶ API Gateway Instance 3
            │
            ├─▶ Auth Service Instance 1
            ├─▶ Auth Service Instance 2
            └─▶ Auth Service Instance 3
```

### Database Scaling
```
MySQL Master (Write)
      │
      ├─▶ MySQL Replica 1 (Read)
      ├─▶ MySQL Replica 2 (Read)
      └─▶ MySQL Replica 3 (Read)
```

### Kafka Scaling
```
Kafka Broker 1 (Leader for Topic A)
Kafka Broker 2 (Leader for Topic B)
Kafka Broker 3 (Leader for Topic C)
```

## Security Architecture

### Authentication Flow
```
1. User → API Gateway (with credentials)
2. API Gateway → Auth Service
3. Auth Service → MySQL (verify credentials)
4. Auth Service → User (JWT token)
5. User → API Gateway (with JWT)
6. API Gateway → Validate JWT → Route to Service
```

### Service-to-Service Communication
```
Currently: Internal Docker network (trusted)
Production: Should use mTLS or service mesh
```

## Monitoring Points

### Health Checks
- Eureka: `/actuator/health`
- Services: `/actuator/health` (if actuator enabled)
- MySQL: `mysqladmin ping`
- Kafka: `kafka-topics --list`

### Metrics to Monitor
- Service registration status (Eureka)
- Request latency (API Gateway)
- Database connection pool (Services)
- Kafka consumer lag (Services)
- Error rates (All services)
- Resource usage (CPU, Memory)

## Disaster Recovery

### Backup Strategy
```
Daily:
├── MySQL databases (mysqldump)
├── Kafka topics (mirror maker)
└── Configuration files

Weekly:
└── Full system snapshot
```

### Recovery Procedure
```
1. Restore MySQL from backup
2. Restore Kafka topics
3. Restart services
4. Verify service registration
5. Test critical flows
```

## Technology Stack Summary

| Layer | Technology |
|-------|-----------|
| Frontend | React |
| API Gateway | Spring Cloud Gateway |
| Service Discovery | Netflix Eureka |
| Microservices | Spring Boot 3.5.5 |
| Event Streaming | Apache Kafka 7.5.0 |
| Database | MySQL 8.0 |
| Containerization | Docker |
| Orchestration | Docker Compose |
| Language | Java 17 |
| Build Tool | Maven |
