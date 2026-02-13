package com.ecom.productservice.events;

import java.time.LocalDateTime;
import java.util.UUID;

public class LowStockEvent {

    private String eventId;
    private String eventType;
    private Long productId;
    private String productName;
    private Integer currentStock;
    private Integer threshold;
    private LocalDateTime timestamp;

    public LowStockEvent() {
    }

    public LowStockEvent(Long productId, String productName, Integer currentStock, Integer threshold) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = "LOW_STOCK_ALERT";
        this.productId = productId;
        this.productName = productName;
        this.currentStock = currentStock;
        this.threshold = threshold;
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

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public Integer getCurrentStock() {
        return currentStock;
    }

    public void setCurrentStock(Integer currentStock) {
        this.currentStock = currentStock;
    }

    public Integer getThreshold() {
        return threshold;
    }

    public void setThreshold(Integer threshold) {
        this.threshold = threshold;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LowStockEvent{eventId='" + eventId + "', productId=" + productId +
                ", productName='" + productName + "', currentStock=" + currentStock +
                ", threshold=" + threshold + "}";
    }
}
