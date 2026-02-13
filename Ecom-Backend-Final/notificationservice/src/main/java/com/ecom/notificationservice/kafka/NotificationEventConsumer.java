package com.ecom.notificationservice.kafka;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.ecom.notificationservice.model.ProcessedEvent;
import com.ecom.notificationservice.repository.ProcessedEventRepository;

@Service
public class NotificationEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationEventConsumer.class);

    private final ProcessedEventRepository processedEventRepository;

    public NotificationEventConsumer(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    // ========================
    // USER_REGISTERED
    // ========================
    @KafkaListener(topics = "USER_REGISTERED", groupId = "notification-service-group")
    public void consumeUserRegistered(Map<String, Object> eventData) {
        String eventId = (String) eventData.get("eventId");
        if (isDuplicate(eventId, "USER_REGISTERED"))
            return;

        String username = (String) eventData.get("username");
        String email = (String) eventData.get("email");

        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  📧 NOTIFICATION: Welcome Email                            ║");
        log.info("║  To: {} ({})                                       ", username, email);
        log.info("║  Subject: Welcome to Ecom!                                 ║");
        log.info("║  Body: Thank you for registering, {}!              ", username);
        log.info("║  Your account has been created successfully.               ║");
        log.info("╚══════════════════════════════════════════════════════════════╝");

        markProcessed(eventId, "USER_REGISTERED");
    }

    // ========================
    // USER_LOGGED_IN
    // ========================
    @KafkaListener(topics = "USER_LOGGED_IN", groupId = "notification-service-group")
    public void consumeUserLoggedIn(Map<String, Object> eventData) {
        String eventId = (String) eventData.get("eventId");
        if (isDuplicate(eventId, "USER_LOGGED_IN"))
            return;

        String username = (String) eventData.get("username");

        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  🔑 NOTIFICATION: Login Alert                              ║");
        log.info("║  User: {} has logged in                            ", username);
        log.info("║  Time: {}                                          ", eventData.get("timestamp"));
        log.info("╚══════════════════════════════════════════════════════════════╝");

        markProcessed(eventId, "USER_LOGGED_IN");
    }

    // ========================
    // LOW_STOCK_ALERT
    // ========================
    @KafkaListener(topics = "LOW_STOCK_ALERT", groupId = "notification-service-group")
    public void consumeLowStockAlert(Map<String, Object> eventData) {
        String eventId = (String) eventData.get("eventId");
        if (isDuplicate(eventId, "LOW_STOCK_ALERT"))
            return;

        String productName = (String) eventData.get("productName");
        Object productId = eventData.get("productId");
        Object currentStock = eventData.get("currentStock");
        Object threshold = eventData.get("threshold");

        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  🚨 ADMIN NOTIFICATION: Low Stock Alert                    ║");
        log.info("║  Product: {} (ID: {})                              ", productName, productId);
        log.info("║  Current Stock: {}                                 ", currentStock);
        log.info("║  Threshold: {}                                     ", threshold);
        log.info("║  Action Required: Please restock immediately!              ║");
        log.info("╚══════════════════════════════════════════════════════════════╝");

        markProcessed(eventId, "LOW_STOCK_ALERT");
    }

    // ========================
    // ORDER_STATUS_UPDATED
    // ========================
    @KafkaListener(topics = "ORDER_STATUS_UPDATED", groupId = "notification-service-group")
    public void consumeOrderStatusUpdated(Map<String, Object> eventData) {
        String eventId = (String) eventData.get("eventId");
        if (isDuplicate(eventId, "ORDER_STATUS_UPDATED"))
            return;

        String username = (String) eventData.get("username");
        Object orderId = eventData.get("orderId");
        String shippingStatus = (String) eventData.get("shippingStatus");

        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  📦 NOTIFICATION: Order Status Update                      ║");
        log.info("║  To: {}                                            ", username);
        log.info("║  Order: #{}                                        ", orderId);
        log.info("║  Shipping Status: {}                               ", shippingStatus);
        log.info("║  Your order shipping status has been updated.              ║");
        log.info("╚══════════════════════════════════════════════════════════════╝");

        markProcessed(eventId, "ORDER_STATUS_UPDATED");
    }

    // ========================
    // Idempotency Helpers
    // ========================
    private boolean isDuplicate(String eventId, String eventType) {
        if (eventId == null) {
            log.warn("Event with null eventId received for type: {}. Processing anyway.", eventType);
            return false;
        }
        if (processedEventRepository.existsByEventId(eventId)) {
            log.warn("Duplicate event detected: eventId={}, type={}. Skipping.", eventId, eventType);
            return true;
        }
        return false;
    }

    private void markProcessed(String eventId, String eventType) {
        if (eventId != null) {
            processedEventRepository.save(new ProcessedEvent(eventId, eventType));
        }
    }
}
