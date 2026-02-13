# 🚀 Run Kafka Flow Tests - Quick Guide

## ⚡ Fastest Way to Test

### PowerShell (Recommended):
```powershell
.\Test-KafkaFlow.ps1
```

### Command Prompt:
```cmd
.\test-complete-kafka-flow.cmd
```

---

## 📋 What Gets Tested

✅ **Kafka Topics** - Are they created?  
✅ **Event Publishing** - Does Auth Service publish events?  
✅ **Event Consumption** - Does Notification Service consume events?  
✅ **Event Processing** - Are notifications logged?  
✅ **Idempotency** - Are duplicates prevented?  

---

## 🎬 What You'll See

### 3 Windows Will Open:
1. **Auth Service Logs** - Shows event publishing
2. **Notification Service Logs** - Shows event processing
3. **Kafka Topic Monitor** - Shows messages in real-time

### In Auth Service Window:
```
Publishing USER_REGISTERED event: UserEvent(...)
```

### In Notification Service Window:
```
╔══════════════════════════════════════════════════════════════╗
║  📧 NOTIFICATION: Welcome Email                            ║
║  To: flowtest_143052 (flowtest_143052@example.com)         ║
╚══════════════════════════════════════════════════════════════╝
```

### In Kafka Monitor Window:
```json
{
  "eventId": "abc123...",
  "eventType": "USER_REGISTERED",
  "username": "flowtest_143052"
}
```

---

## ✅ Success = You See All Three

If you see:
- Auth Service publishing events ✅
- Kafka storing messages ✅
- Notification Service processing events ✅

**Then your Kafka flow is working perfectly!** 🎉

---

## 🔧 Quick Commands

### Just Check Topics:
```cmd
.\test-kafka-topics.cmd
```

### Just Check Processing:
```cmd
.\test-notification-processing.cmd
```

### Full Flow Test:
```powershell
.\Test-KafkaFlow.ps1
```

---

## 📖 Need More Info?

- **Detailed guide:** `KAFKA-FLOW-TESTS.md`
- **All tests:** `README.md`
- **Quick summary:** `QUICK-SUMMARY.md`

---

**Ready?** Run `.\Test-KafkaFlow.ps1` now! 🚀
