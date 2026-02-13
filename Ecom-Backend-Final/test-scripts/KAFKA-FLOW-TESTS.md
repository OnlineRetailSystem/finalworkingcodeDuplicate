# Kafka Flow Tests - Complete Guide

## 🎯 What These Tests Do

These tests verify the complete Kafka event flow in your backend:

1. **Topic Creation** - Checks if Kafka topics are created
2. **Event Publishing** - Verifies Auth Service publishes events
3. **Event Consumption** - Confirms Notification Service consumes events
4. **Event Processing** - Validates events are processed correctly
5. **Idempotency** - Ensures no duplicate processing

---

## 🚀 Quick Start

### For PowerShell (Recommended):
```powershell
cd D:\Synechron\final project new attempt\finalworkingcodeDuplicate\Ecom-Backend-Final\test-scripts
.\Test-KafkaFlow.ps1
```

### For Command Prompt:
```cmd
cd D:\Synechron\final project new attempt\finalworkingcodeDuplicate\Ecom-Backend-Final\test-scripts
.\test-complete-kafka-flow.cmd
```

---

## 📋 Available Test Scripts

### 1. `Test-KafkaFlow.ps1` (PowerShell - Best Experience)
**What it does:**
- ✅ Checks all services are running
- ✅ Shows current Kafka topics
- ✅ Opens monitoring windows (Auth logs, Notification logs, Kafka topic)
- ✅ Registers a test user (triggers USER_REGISTERED event)
- ✅ Logs in test user (triggers USER_LOGGED_IN event)
- ✅ Verifies events were processed
- ✅ Shows consumer group status
- ✅ Provides detailed summary

**Run:**
```powershell
.\Test-KafkaFlow.ps1
```

**Time:** 5-7 minutes

---

### 2. `test-complete-kafka-flow.cmd` (Command Prompt)
**What it does:**
- Same as PowerShell version but for CMD
- Opens multiple windows to monitor the flow
- Tests complete event lifecycle

**Run:**
```cmd
.\test-complete-kafka-flow.cmd
```

**Time:** 5-7 minutes

---

### 3. `test-kafka-topics.cmd` (Quick Topic Check)
**What it does:**
- ✅ Lists all Kafka topics
- ✅ Checks for expected topics
- ✅ Shows topic details
- ✅ Shows consumer groups

**Run:**
```cmd
.\test-kafka-topics.cmd
```

**Time:** 1-2 minutes

---

### 4. `test-notification-processing.cmd` (Processing Verification)
**What it does:**
- ✅ Checks notification service health
- ✅ Shows current processed events
- ✅ Triggers events and monitors processing
- ✅ Verifies events appear in database

**Run:**
```cmd
.\test-notification-processing.cmd
```

**Time:** 3-4 minutes

---

## 🎬 What You'll See

### Phase 1: Pre-flight Checks
```
✓ Kafka: RUNNING
✓ Auth Service: RUNNING
✓ Notification Service: RUNNING
```

### Phase 2: Initial State
```
Current Kafka topics:
  - USER_REGISTERED
  - USER_LOGGED_IN
  - PAYMENT_SUCCESS
  (or empty if no events yet)

Current processed events:
  [list of previously processed events]
```

### Phase 3: Monitoring Windows Open
Three windows will open:
1. **Auth Service Logs** - Shows event publishing
2. **Notification Service Logs** - Shows event consumption
3. **Kafka Topic Monitor** - Shows messages in real-time

### Phase 4: Events Triggered
```
Registering user: flowtest_143052
✓ User registered successfully!

Watch the windows:
1. Auth Service: "Publishing USER_REGISTERED event"
2. Kafka Monitor: Event message appears
3. Notification Service: "Welcome Email" notification

Logging in user: flowtest_143052
✓ User logged in successfully!

Watch the windows:
1. Auth Service: "Publishing USER_LOGGED_IN event"
2. Notification Service: "Login Alert" notification
```

### Phase 5: Verification
```
Topics created:
  ✓ USER_REGISTERED
  ✓ USER_LOGGED_IN

Processed events:
  {
    "eventId": "abc123...",
    "eventType": "USER_REGISTERED",
    "processedAt": "2026-02-14T10:30:00"
  },
  {
    "eventId": "def456...",
    "eventType": "USER_LOGGED_IN",
    "processedAt": "2026-02-14T10:30:10"
  }

Total events processed: 2
```

---

## 📺 Monitoring Windows Explained

### Window 1: Auth Service Logs (Producer)
**What to look for:**
```
Publishing USER_REGISTERED event: UserEvent(
  eventId=abc123...,
  username=flowtest_143052,
  email=flowtest_143052@example.com
)
```

This shows the Auth Service is successfully publishing events to Kafka.

### Window 2: Notification Service Logs (Consumer)
**What to look for:**
```
╔══════════════════════════════════════════════════════════════╗
║  📧 NOTIFICATION: Welcome Email                            ║
║  To: flowtest_143052 (flowtest_143052@example.com)         ║
║  Subject: Welcome to Ecom!                                 ║
║  Body: Thank you for registering, flowtest_143052!         ║
╚══════════════════════════════════════════════════════════════╝
```

This shows the Notification Service is successfully consuming and processing events.

### Window 3: Kafka Topic Monitor
**What to look for:**
```
{
  "eventId": "abc123...",
  "eventType": "USER_REGISTERED",
  "username": "flowtest_143052",
  "email": "flowtest_143052@example.com",
  "timestamp": "2026-02-14T10:30:00"
}
```

This shows the actual message stored in Kafka.

---

## ✅ Success Indicators

Your Kafka flow is working correctly if you see:

- [x] All services respond to health checks
- [x] Kafka topics are created (or auto-create on first event)
- [x] Auth Service logs show "Publishing ... event"
- [x] Kafka topic monitor shows event messages
- [x] Notification Service logs show formatted notifications
- [x] Processed events endpoint returns events
- [x] No duplicate processing (same eventId not processed twice)
- [x] Consumer group is active

---

## 🔍 Manual Verification Commands

### Check Kafka Topics
```bash
docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Monitor Specific Topic
```bash
# USER_REGISTERED
docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_REGISTERED --from-beginning

# USER_LOGGED_IN
docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_LOGGED_IN --from-beginning
```

### Check Consumer Group
```bash
docker exec ecom-kafka kafka-consumer-groups --describe --group notification-service-group --bootstrap-server localhost:9092
```

### Check Processed Events
```bash
curl http://localhost:8089/notifications/processed-events
```

### Check Database Directly
```bash
# Processed events
docker exec ecom-mysql mysql -uroot -proot -e "SELECT * FROM notificationdb.processed_events;"

# Registered users
docker exec ecom-mysql mysql -uroot -proot -e "SELECT username, email FROM authservice.users;"
```

---

## 🎯 Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    USER REGISTRATION FLOW                    │
└─────────────────────────────────────────────────────────────┘

1. Test Script
      │
      ├─► POST /auth/signup
      │
      ▼
2. API Gateway (8090)
      │
      ▼
3. Auth Service (8087)
      │
      ├─► Save to MySQL (authservice.users)
      │
      └─► Kafka Producer
            │
            ├─► Create event with unique eventId
            │
            └─► Publish to topic: USER_REGISTERED
                  │
                  ▼
4. Kafka Broker (9092)
      │
      ├─► Store message in topic
      │
      └─► Notify consumers
            │
            ▼
5. Notification Service (8089)
      │
      ├─► Kafka Consumer receives event
      │
      ├─► Check idempotency (processed_events table)
      │   │
      │   ├─► If eventId exists: Skip (duplicate)
      │   │
      │   └─► If eventId new: Process
      │
      ├─► Log formatted notification
      │   ╔════════════════════════════════════╗
      │   ║  📧 NOTIFICATION: Welcome Email    ║
      │   ╚════════════════════════════════════╝
      │
      └─► Save to processed_events table
            │
            └─► Prevent future duplicates
```

---

## 🐛 Troubleshooting

### Problem: "Kafka is not running"
**Solution:**
```bash
docker-compose up -d kafka zookeeper
# Wait 30 seconds
docker logs ecom-kafka
```

### Problem: "No topics found"
**Solution:**
This is normal! Topics are auto-created when first event is published.
Run the test to trigger topic creation.

### Problem: "Notification service not responding"
**Solution:**
```bash
docker logs ecom-notificationservice
# Check for errors
docker-compose restart notificationservice
```

### Problem: "Events not appearing in processed_events"
**Solution:**
1. Check notification service logs for errors
2. Verify Kafka consumer is connected
3. Check consumer group status:
```bash
docker exec ecom-kafka kafka-consumer-groups --describe --group notification-service-group --bootstrap-server localhost:9092
```

### Problem: "Monitoring windows don't open"
**Solution:**
Open them manually:
```bash
# Auth Service
docker logs ecom-authservice -f

# Notification Service
docker logs ecom-notificationservice -f

# Kafka Topic
docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_REGISTERED --from-beginning
```

---

## 📊 Expected Results

### ✅ What Should Work

| Test | Expected Result |
|------|----------------|
| Service Health Checks | All services respond |
| Topic Creation | Topics created or auto-create |
| Event Publishing | Auth service logs show publishing |
| Event Storage | Kafka topic monitor shows messages |
| Event Consumption | Notification service receives events |
| Event Processing | Notifications logged to console |
| Idempotency | Same eventId not processed twice |
| Database Storage | Events saved to processed_events |

### ❌ What Won't Work (Expected)

| Feature | Status | Reason |
|---------|--------|--------|
| Frontend Notifications | Not Working | No WebSocket/SSE implemented |
| JWT Tokens | Not Working | Using Basic Auth instead |
| Email Sending | Not Working | Only console logging |

---

## 🎓 Understanding the Output

### Auth Service Log Entry
```
2026-02-14 10:30:00.123  INFO  AuthEventProducer : Publishing USER_REGISTERED event: UserEvent(eventId=abc123, username=flowtest_143052, email=flowtest_143052@example.com, roles=ROLE_USER, timestamp=2026-02-14T10:30:00)
```

**Meaning:** Auth service created an event and sent it to Kafka.

### Kafka Message
```json
{
  "eventId": "abc123-def456-ghi789",
  "eventType": "USER_REGISTERED",
  "username": "flowtest_143052",
  "email": "flowtest_143052@example.com",
  "roles": "ROLE_USER",
  "timestamp": "2026-02-14T10:30:00"
}
```

**Meaning:** This is the actual message stored in Kafka topic.

### Notification Service Log Entry
```
2026-02-14 10:30:05.456  INFO  NotificationEventConsumer : 
╔══════════════════════════════════════════════════════════════╗
║  📧 NOTIFICATION: Welcome Email                            ║
║  To: flowtest_143052 (flowtest_143052@example.com)         ║
║  Subject: Welcome to Ecom!                                 ║
║  Body: Thank you for registering, flowtest_143052!         ║
║  Your account has been created successfully.               ║
╚══════════════════════════════════════════════════════════════╝
```

**Meaning:** Notification service consumed the event and processed it.

---

## 🎯 Next Steps

After confirming Kafka flow works:

1. **Implement JWT** - Add token-based authentication
2. **Add WebSocket** - Enable real-time frontend notifications
3. **Implement Email** - Send actual emails instead of logging
4. **Add More Events** - Implement payment, order, inventory events

---

## 📞 Need Help?

- **Quick overview:** Read `QUICK-SUMMARY.md`
- **Detailed analysis:** Read `TEST-REPORT.md`
- **All tests:** Read `README.md`
- **Navigation:** Read `INDEX.md`

---

**Remember:** These tests verify your Kafka implementation is working correctly. The backend event processing is solid! 🚀
