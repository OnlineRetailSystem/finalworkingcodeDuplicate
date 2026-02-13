# ============================================================
# Complete Kafka Flow Test (PowerShell)
# Tests: Topic Creation -> Event Publishing -> Event Consumption
# ============================================================

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "          COMPLETE KAFKA FLOW VERIFICATION TEST" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

$API_GATEWAY = "http://localhost:8090"
$NOTIFICATION_SERVICE = "http://localhost:8089"

# ============================================================
# PHASE 1: Pre-flight checks
# ============================================================
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host "PHASE 1: Pre-flight Checks" -ForegroundColor Yellow
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host ""

Write-Host "[1.1] Checking Kafka..." -ForegroundColor White
try {
    docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092 | Out-Null
    Write-Host "✓ Kafka: RUNNING" -ForegroundColor Green
} catch {
    Write-Host "✗ ERROR: Kafka is not running!" -ForegroundColor Red
    pause
    exit 1
}

Write-Host ""
Write-Host "[1.2] Checking Auth Service..." -ForegroundColor White
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8087" -Method Get -TimeoutSec 5 -ErrorAction SilentlyContinue
    Write-Host "✓ Auth Service: RUNNING" -ForegroundColor Green
} catch {
    Write-Host "✗ ERROR: Auth service is not responding!" -ForegroundColor Red
    pause
    exit 1
}

Write-Host ""
Write-Host "[1.3] Checking Notification Service..." -ForegroundColor White
try {
    $response = Invoke-RestMethod -Uri "$NOTIFICATION_SERVICE/notifications/health" -Method Get
    Write-Host "✓ Notification Service: RUNNING" -ForegroundColor Green
    Write-Host "  Response: $response" -ForegroundColor Gray
} catch {
    Write-Host "✗ ERROR: Notification service is not responding!" -ForegroundColor Red
    pause
    exit 1
}

Write-Host ""
Write-Host "✓ All services are ready!" -ForegroundColor Green
Write-Host ""
pause

# ============================================================
# PHASE 2: Check initial state
# ============================================================
Write-Host ""
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host "PHASE 2: Initial State" -ForegroundColor Yellow
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host ""

Write-Host "[2.1] Current Kafka topics:" -ForegroundColor White
docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092
Write-Host ""

Write-Host "[2.2] Current processed events:" -ForegroundColor White
try {
    $events = Invoke-RestMethod -Uri "$NOTIFICATION_SERVICE/notifications/processed-events" -Method Get
    $events | ConvertTo-Json -Depth 3
} catch {
    Write-Host "No events processed yet" -ForegroundColor Gray
}
Write-Host ""
pause

# ============================================================
# PHASE 3: Open monitoring windows
# ============================================================
Write-Host ""
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host "PHASE 3: Opening Monitoring Windows" -ForegroundColor Yellow
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host ""

Write-Host "Opening Auth Service logs..." -ForegroundColor White
Start-Process powershell -ArgumentList "-NoExit", "-Command", "docker logs ecom-authservice -f"
Start-Sleep -Seconds 2

Write-Host "Opening Notification Service logs..." -ForegroundColor White
Start-Process powershell -ArgumentList "-NoExit", "-Command", "docker logs ecom-notificationservice -f"
Start-Sleep -Seconds 2

Write-Host "Opening Kafka topic monitor for USER_REGISTERED..." -ForegroundColor White
Start-Process powershell -ArgumentList "-NoExit", "-Command", "docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_REGISTERED --from-beginning"
Start-Sleep -Seconds 2

Write-Host ""
Write-Host "✓ All monitoring windows opened!" -ForegroundColor Green
Write-Host "Keep them visible to see the flow in real-time." -ForegroundColor Cyan
Write-Host ""
pause

# ============================================================
# PHASE 4: Trigger events
# ============================================================
Write-Host ""
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host "PHASE 4: Triggering Events" -ForegroundColor Yellow
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host ""

$timestamp = Get-Date -Format "HHmmss"
$random = Get-Random -Maximum 9999
$TEST_USER = "flowtest_$timestamp$random"
$TEST_EMAIL = "flowtest_$timestamp$random@example.com"

Write-Host "[4.1] Registering user to trigger USER_REGISTERED event..." -ForegroundColor White
Write-Host ""
Write-Host "Username: $TEST_USER" -ForegroundColor Cyan
Write-Host "Email: $TEST_EMAIL" -ForegroundColor Cyan
Write-Host ""

$signupBody = @{
    username = $TEST_USER
    password = "Test@123"
    email = $TEST_EMAIL
    firstName = "Flow"
    lastName = "Test"
} | ConvertTo-Json

try {
    $signupResponse = Invoke-RestMethod -Uri "$API_GATEWAY/auth/signup" -Method Post -Body $signupBody -ContentType "application/json"
    Write-Host "✓ User registered successfully!" -ForegroundColor Green
    Write-Host "  Response: $signupResponse" -ForegroundColor Gray
} catch {
    Write-Host "✗ Registration failed: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "Event triggered! Watch the monitoring windows:" -ForegroundColor Cyan
Write-Host "1. Auth Service Logs: Should show 'Publishing USER_REGISTERED event'" -ForegroundColor Gray
Write-Host "2. Kafka Topic Monitor: Should show the event message" -ForegroundColor Gray
Write-Host "3. Notification Service Logs: Should show 'Welcome Email' notification" -ForegroundColor Gray
Write-Host ""
Write-Host "Waiting 10 seconds for processing..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host ""
Write-Host "[4.2] Logging in user to trigger USER_LOGGED_IN event..." -ForegroundColor White
Write-Host ""

$signinBody = @{
    username = $TEST_USER
    password = "Test@123"
} | ConvertTo-Json

try {
    $signinResponse = Invoke-RestMethod -Uri "$API_GATEWAY/auth/signin" -Method Post -Body $signinBody -ContentType "application/json"
    Write-Host "✓ User logged in successfully!" -ForegroundColor Green
    Write-Host "  Response: $signinResponse" -ForegroundColor Gray
} catch {
    Write-Host "✗ Login failed: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "Login event triggered! Watch the monitoring windows:" -ForegroundColor Cyan
Write-Host "1. Auth Service Logs: Should show 'Publishing USER_LOGGED_IN event'" -ForegroundColor Gray
Write-Host "2. Notification Service Logs: Should show 'Login Alert' notification" -ForegroundColor Gray
Write-Host ""
Write-Host "Waiting 10 seconds for processing..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# ============================================================
# PHASE 5: Verify processing
# ============================================================
Write-Host ""
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host "PHASE 5: Verifying Event Processing" -ForegroundColor Yellow
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host ""

Write-Host "[5.1] Checking if topics were created..." -ForegroundColor White
Write-Host ""
$topics = docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092
$topics | Where-Object { $_ -match "USER_REGISTERED|USER_LOGGED_IN" } | ForEach-Object {
    Write-Host "  ✓ $_" -ForegroundColor Green
}
Write-Host ""

Write-Host "[5.2] Checking processed events in database..." -ForegroundColor White
Write-Host ""
try {
    $processedEvents = Invoke-RestMethod -Uri "$NOTIFICATION_SERVICE/notifications/processed-events" -Method Get
    Write-Host "Processed Events:" -ForegroundColor Cyan
    $processedEvents | ConvertTo-Json -Depth 3
    Write-Host ""
    Write-Host "Total events processed: $($processedEvents.Count)" -ForegroundColor Green
} catch {
    Write-Host "Could not retrieve processed events" -ForegroundColor Red
}
Write-Host ""

# ============================================================
# PHASE 6: Check consumer group
# ============================================================
Write-Host ""
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host "PHASE 6: Kafka Consumer Group Status" -ForegroundColor Yellow
Write-Host "================================================================" -ForegroundColor Yellow
Write-Host ""

Write-Host "[6.1] Listing consumer groups..." -ForegroundColor White
docker exec ecom-kafka kafka-consumer-groups --list --bootstrap-server localhost:9092
Write-Host ""

Write-Host "[6.2] Notification service consumer group details..." -ForegroundColor White
docker exec ecom-kafka kafka-consumer-groups --describe --group notification-service-group --bootstrap-server localhost:9092
Write-Host ""

# ============================================================
# PHASE 7: Summary
# ============================================================
Write-Host ""
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "                    FLOW VERIFICATION SUMMARY" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Test User: $TEST_USER" -ForegroundColor White
Write-Host "Test Email: $TEST_EMAIL" -ForegroundColor White
Write-Host ""

Write-Host "VERIFICATION CHECKLIST:" -ForegroundColor Yellow
Write-Host ""
Write-Host "[ ] Auth Service Logs show 'Publishing USER_REGISTERED event'" -ForegroundColor Gray
Write-Host "[ ] Auth Service Logs show 'Publishing USER_LOGGED_IN event'" -ForegroundColor Gray
Write-Host "[ ] Kafka Topic Monitor shows event messages" -ForegroundColor Gray
Write-Host "[ ] Notification Service Logs show 'Welcome Email' notification" -ForegroundColor Gray
Write-Host "[ ] Notification Service Logs show 'Login Alert' notification" -ForegroundColor Gray
Write-Host "[ ] Processed events endpoint shows both events" -ForegroundColor Gray
Write-Host "[ ] No duplicate processing occurred" -ForegroundColor Gray
Write-Host "[ ] Consumer group is active and consuming" -ForegroundColor Gray
Write-Host ""

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host "                    TEST COMPLETE" -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "The complete Kafka flow has been tested!" -ForegroundColor Green
Write-Host ""
Write-Host "Review the monitoring windows to see:" -ForegroundColor Cyan
Write-Host "  - Auth Service publishing events" -ForegroundColor Gray
Write-Host "  - Kafka storing events" -ForegroundColor Gray
Write-Host "  - Notification Service consuming events" -ForegroundColor Gray
Write-Host "  - Events being processed and logged" -ForegroundColor Gray
Write-Host ""
Write-Host "All windows will remain open for your review." -ForegroundColor Yellow
Write-Host "Close them when done." -ForegroundColor Yellow
Write-Host ""
pause
