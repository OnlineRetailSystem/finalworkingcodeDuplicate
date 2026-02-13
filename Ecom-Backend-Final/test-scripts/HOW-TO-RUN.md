# How to Run Test Scripts - Simple Guide

## 🎯 For Complete Beginners

### Step 1: Open Command Prompt
1. Press `Windows Key + R`
2. Type `cmd`
3. Press Enter

### Step 2: Navigate to Project
```cmd
cd D:\Synechron\final project new attempt\finalworkingcodeDuplicate\Ecom-Backend-Final
```

### Step 3: Start Docker Services (if not already running)
```cmd
docker-compose up -d
```

**Wait 2-3 minutes** for services to start.

### Step 4: Go to Test Scripts Folder
```cmd
cd test-scripts
```

### Step 5: Run Tests
```cmd
run-all-tests.cmd
```

**That's it!** The script will:
- Check if services are running
- Run all tests
- Show you results
- Open log windows
- Generate reports

---

## 🚀 Quick Commands

### Option 1: Run Everything (Recommended)
```cmd
cd D:\Synechron\final project new attempt\finalworkingcodeDuplicate\Ecom-Backend-Final\test-scripts
run-all-tests.cmd
```

### Option 2: Just Check Status
```cmd
cd D:\Synechron\final project new attempt\finalworkingcodeDuplicate\Ecom-Backend-Final\test-scripts
check-system-status.cmd
```

### Option 3: Test Kafka Only
```cmd
cd D:\Synechron\final project new attempt\finalworkingcodeDuplicate\Ecom-Backend-Final\test-scripts
test-kafka-flow.cmd
```

---

## 📺 What You'll See

### When Running Tests:
```
================================================================
          E-COMMERCE BACKEND COMPREHENSIVE TEST SUITE
================================================================

This script will run all tests to verify:
  1. Kafka event-driven architecture
  2. Authentication and security
  3. Frontend notification delivery

Press any key to start testing...
```

### Test Results:
```
✓ PASSED: User registration API call
✓ PASSED: Kafka event published
✓ PASSED: Notification service received event
✗ FAILED: JWT token generation (Not Implemented)
✗ FAILED: Frontend notification delivery (Not Implemented)
```

### Final Summary:
```
KAFKA IMPLEMENTATION:
  Status: WORKING ✅
  
JWT IMPLEMENTATION:
  Status: NOT IMPLEMENTED ❌
  
FRONTEND NOTIFICATIONS:
  Status: NOT WORKING ❌
```

---

## 🔧 Troubleshooting

### Problem: "docker-compose is not recognized"
**Solution:** Docker is not installed or not in PATH
```cmd
# Check if Docker is installed
docker --version

# If not installed, install Docker Desktop for Windows
```

### Problem: "Cannot connect to services"
**Solution:** Services are not running
```cmd
# Start services
cd D:\Synechron\final project new attempt\finalworkingcodeDuplicate\Ecom-Backend-Final
docker-compose up -d

# Wait 2-3 minutes, then check status
docker-compose ps
```

### Problem: "curl is not recognized"
**Solution:** curl is not available (rare on Windows 10+)
```cmd
# Check Windows version
winver

# If Windows 10 1803 or later, curl should be available
# Otherwise, install curl or use PowerShell version
```

### Problem: Services show as "unhealthy"
**Solution:** Wait longer or restart
```cmd
# Check logs
docker-compose logs

# Restart specific service
docker-compose restart authservice

# Or restart all
docker-compose restart
```

---

## 📖 Reading Results

### After Running Tests:

1. **Console Output** - Shows test results in real-time
2. **Log Windows** - Separate windows open showing service logs
3. **QUICK-SUMMARY.md** - Open this for quick overview
4. **TEST-REPORT.md** - Open this for detailed analysis

### To Read Reports:
```cmd
# Open in Notepad
notepad QUICK-SUMMARY.md

# Or open in VS Code
code QUICK-SUMMARY.md

# Or just double-click the file in File Explorer
```

---

## 🎓 What Each Script Does

### `run-all-tests.cmd`
- Checks if services are running
- Tests Kafka event flow
- Tests authentication
- Tests frontend notifications
- Opens service logs
- Shows summary

**Time:** 5-10 minutes

### `check-system-status.cmd`
- Quick health check
- Shows which services are up/down
- Lists Kafka topics
- Shows recent logs

**Time:** 1 minute

### `test-kafka-flow.cmd`
- Registers test user
- Logs in test user
- Checks Kafka events
- Verifies notifications

**Time:** 2-3 minutes

### `test-auth-security.cmd`
- Tests registration
- Tests login
- Tests protected endpoints
- Checks for JWT

**Time:** 2 minutes

### `test-frontend-notifications.cmd`
- Checks frontend
- Tests notification service
- Verifies real-time delivery

**Time:** 2 minutes

---

## 💡 Tips

### Tip 1: Run from File Explorer
1. Open File Explorer
2. Navigate to: `D:\Synechron\final project new attempt\finalworkingcodeDuplicate\Ecom-Backend-Final\test-scripts`
3. Double-click `run-all-tests.cmd`

### Tip 2: Create Desktop Shortcut
1. Right-click `run-all-tests.cmd`
2. Select "Create shortcut"
3. Move shortcut to Desktop
4. Double-click to run anytime

### Tip 3: Keep Logs Open
The test scripts open log windows automatically. Keep them open to see real-time Kafka events!

### Tip 4: Run Multiple Times
You can run tests multiple times. Each run creates new test users to avoid conflicts.

---

## ✅ Success Checklist

After running tests, you should see:

- [ ] All Docker containers running
- [ ] Kafka topics listed
- [ ] Test user registered successfully
- [ ] Notification logged in backend
- [ ] Summary showing Kafka ✅, JWT ❌, Frontend ❌
- [ ] Log windows showing events

---

## 🎯 Next Steps After Running Tests

1. **Read QUICK-SUMMARY.md** - Understand what's working
2. **Read TEST-REPORT.md** - Get detailed analysis
3. **Check service logs** - See Kafka events in action
4. **Implement JWT** - Follow recommendations
5. **Add WebSocket** - Enable frontend notifications

---

## 📞 Still Need Help?

### Check These Files:
- `INDEX.md` - Navigation guide
- `README.md` - Complete documentation
- `QUICK-SUMMARY.md` - Quick overview
- `TEST-REPORT.md` - Detailed analysis

### Common Issues:
1. Services not running → Run `docker-compose up -d`
2. Port conflicts → Check docker-compose.yml
3. Curl not found → Use Windows 10+ or install curl
4. Tests fail → Check service logs

---

**Remember:** These tests don't change your code. They only verify what's working and what's not!

**Good luck!** 🚀
