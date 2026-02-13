#!/bin/bash

# ============================================================
# Kafka Event Flow Test Script
# Tests the complete event-driven architecture
# ============================================================

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║         KAFKA EVENT FLOW INTEGRATION TEST                    ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

API_GATEWAY="http://localhost:8090"
NOTIFICATION_SERVICE="http://localhost:8089"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Function to print test result
print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ PASSED${NC}: $2"
        ((TESTS_PASSED++))
    else
        echo -e "${RED}✗ FAILED${NC}: $2"
        ((TESTS_FAILED++))
    fi
}

# Function to wait for Kafka event processing
wait_for_kafka() {
    echo -e "${YELLOW}⏳ Waiting for Kafka event processing (5 seconds)...${NC}"
    sleep 5
}

echo "============================================================"
echo "TEST 1: User Registration Event Flow"
echo "============================================================"
echo "Testing: Auth Service → Kafka → Notification Service"
echo ""

# Generate unique username
TIMESTAMP=$(date +%s)
TEST_USER="testuser_${TIMESTAMP}"
TEST_EMAIL="test_${TIMESTAMP}@example.com"

echo "Registering user: $TEST_USER"
SIGNUP_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$API_GATEWAY/auth/signup" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"$TEST_USER\",
    \"password\": \"Test@123\",
    \"email\": \"$TEST_EMAIL\",
    \"firstName\": \"Test\",
    \"lastName\": \"User\"
  }")

HTTP_CODE=$(echo "$SIGNUP_RESPONSE" | tail -n1)
RESPONSE_BODY=$(echo "$SIGNUP_RESPONSE" | head -n-1)

if [ "$HTTP_CODE" -eq 200 ]; then
    print_result 0 "User registration API call"
    echo "Response: $RESPONSE_BODY"
else
    print_result 1 "User registration API call (HTTP $HTTP_CODE)"
    echo "Response: $RESPONSE_BODY"
fi

wait_for_kafka

echo ""
echo "Checking notification service logs for USER_REGISTERED event..."
echo "Expected: Welcome email notification in logs"
echo ""

echo "============================================================"
echo "TEST 2: User Login Event Flow"
echo "============================================================"
echo "Testing: Auth Service → Kafka → Notification Service"
echo ""

echo "Logging in user: $TEST_USER"
SIGNIN_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$API_GATEWAY/auth/signin" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"$TEST_USER\",
    \"password\": \"Test@123\"
  }")

HTTP_CODE=$(echo "$SIGNIN_RESPONSE" | tail -n1)
RESPONSE_BODY=$(echo "$SIGNIN_RESPONSE" | head -n-1)

if [ "$HTTP_CODE" -eq 200 ]; then
    print_result 0 "User login API call"
    echo "Response: $RESPONSE_BODY"
else
    print_result 1 "User login API call (HTTP $HTTP_CODE)"
    echo "Response: $RESPONSE_BODY"
fi

wait_for_kafka

echo ""
echo "Checking notification service logs for USER_LOGGED_IN event..."
echo "Expected: Login alert notification in logs"
echo ""

echo "============================================================"
echo "TEST 3: Payment Success → Order Creation Event Flow"
echo "============================================================"
echo "Testing: Payment Service → Kafka → Order Service → Kafka → Notification Service"
echo ""

echo "Creating payment intent and processing payment..."
# Note: This requires Stripe integration to be configured
echo "⚠️  Manual test required: Process a payment through the frontend"
echo "Expected flow:"
echo "  1. Payment Service publishes PAYMENT_SUCCESS event"
echo "  2. Order Service consumes event and creates order"
echo "  3. Order Service publishes ORDER_PLACED event"
echo "  4. Notification Service consumes and logs notification"
echo ""

echo "============================================================"
echo "TEST 4: Low Stock Alert Event Flow"
echo "============================================================"
echo "Testing: Product Service → Kafka → Notification Service"
echo ""

echo "⚠️  Manual test required: Reduce product stock below threshold"
echo "Expected flow:"
echo "  1. Product Service detects low stock"
echo "  2. Product Service publishes LOW_STOCK_ALERT event"
echo "  3. Notification Service consumes and logs admin alert"
echo ""

echo "============================================================"
echo "TEST 5: Order Status Update Event Flow"
echo "============================================================"
echo "Testing: Order Service → Kafka → Notification Service"
echo ""

echo "⚠️  Manual test required: Update order shipping status"
echo "Expected flow:"
echo "  1. Order Service publishes ORDER_STATUS_UPDATED event"
echo "  2. Notification Service consumes and logs notification"
echo ""

echo "============================================================"
echo "TEST 6: Verify Processed Events (Idempotency)"
echo "============================================================"
echo "Checking notification service processed events..."
echo ""

PROCESSED_EVENTS=$(curl -s "$NOTIFICATION_SERVICE/notifications/processed-events")
echo "Processed Events Response:"
echo "$PROCESSED_EVENTS" | python3 -m json.tool 2>/dev/null || echo "$PROCESSED_EVENTS"

if echo "$PROCESSED_EVENTS" | grep -q "eventId"; then
    print_result 0 "Notification service tracking processed events"
else
    print_result 1 "Notification service processed events endpoint"
fi

echo ""
echo "============================================================"
echo "TEST 7: Kafka Topics Verification"
echo "============================================================"
echo "Checking if Kafka topics exist..."
echo ""

echo "Expected topics:"
echo "  - USER_REGISTERED"
echo "  - USER_LOGGED_IN"
echo "  - PAYMENT_SUCCESS"
echo "  - ORDER_PLACED"
echo "  - ORDER_STATUS_UPDATED"
echo "  - LOW_STOCK_ALERT"
echo ""

echo "To verify topics, run:"
echo "docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092"
echo ""

echo "============================================================"
echo "TEST SUMMARY"
echo "============================================================"
echo -e "${GREEN}Tests Passed: $TESTS_PASSED${NC}"
echo -e "${RED}Tests Failed: $TESTS_FAILED${NC}"
echo ""

echo "============================================================"
echo "MANUAL VERIFICATION STEPS"
echo "============================================================"
echo ""
echo "1. Check Notification Service Logs:"
echo "   docker logs ecom-notificationservice -f"
echo ""
echo "2. Check Auth Service Logs:"
echo "   docker logs ecom-authservice -f"
echo ""
echo "3. Check Order Service Logs:"
echo "   docker logs ecom-orderservice -f"
echo ""
echo "4. Check Payment Service Logs:"
echo "   docker logs ecom-paymentservice -f"
echo ""
echo "5. List Kafka Topics:"
echo "   docker exec ecom-kafka kafka-topics --list --bootstrap-server localhost:9092"
echo ""
echo "6. Monitor Kafka Messages (example for USER_REGISTERED):"
echo "   docker exec ecom-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic USER_REGISTERED --from-beginning"
echo ""

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    TEST EXECUTION COMPLETE                   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
