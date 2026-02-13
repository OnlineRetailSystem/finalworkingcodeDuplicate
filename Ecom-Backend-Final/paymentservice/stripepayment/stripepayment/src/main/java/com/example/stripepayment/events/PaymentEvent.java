package com.example.stripepayment.events;

import java.time.LocalDateTime;
import java.util.UUID;

public class PaymentEvent {

    private String eventId;
    private String eventType; // PAYMENT_SUCCESS
    private String username;
    private Long productId;
    private Integer quantity;
    private Long amountPaid; // in cents
    private String currency;
    private String paymentIntentId;
    private LocalDateTime timestamp;

    public PaymentEvent() {
    }

    public PaymentEvent(String username, Long productId, Integer quantity,
            Long amountPaid, String currency, String paymentIntentId) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = "PAYMENT_SUCCESS";
        this.username = username;
        this.productId = productId;
        this.quantity = quantity;
        this.amountPaid = amountPaid;
        this.currency = currency;
        this.paymentIntentId = paymentIntentId;
        this.timestamp = LocalDateTime.now();
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Long getAmountPaid() {
        return amountPaid;
    }

    public void setAmountPaid(Long amountPaid) {
        this.amountPaid = amountPaid;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getPaymentIntentId() {
        return paymentIntentId;
    }

    public void setPaymentIntentId(String paymentIntentId) {
        this.paymentIntentId = paymentIntentId;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "PaymentEvent{" +
                "eventId='" + eventId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", username='" + username + '\'' +
                ", productId=" + productId +
                ", quantity=" + quantity +
                ", amountPaid=" + amountPaid +
                ", currency='" + currency + '\'' +
                ", paymentIntentId='" + paymentIntentId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
