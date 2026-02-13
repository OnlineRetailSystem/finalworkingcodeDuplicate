package com.ecom.orderservice.events;

import java.time.LocalDateTime;
import java.util.UUID;

public class OrderEvent {

    private String eventId;
    private String eventType; // ORDER_PLACED, ORDER_STATUS_UPDATED
    private Long orderId;
    private String username;
    private Long productId;
    private String category;
    private Integer quantity;
    private Double totalPrice;
    private String orderStatus;
    private String shippingStatus;
    private LocalDateTime timestamp;

    public OrderEvent() {}

    public OrderEvent(String eventType, Long orderId, String username, Long productId,
                      String category, Integer quantity, Double totalPrice,
                      String orderStatus, String shippingStatus) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.orderId = orderId;
        this.username = username;
        this.productId = productId;
        this.category = category;
        this.quantity = quantity;
        this.totalPrice = totalPrice;
        this.orderStatus = orderStatus;
        this.shippingStatus = shippingStatus;
        this.timestamp = LocalDateTime.now();
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    public Long getProductId() { return productId; }
    public void setProductId(Long productId) { this.productId = productId; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }

    public Double getTotalPrice() { return totalPrice; }
    public void setTotalPrice(Double totalPrice) { this.totalPrice = totalPrice; }

    public String getOrderStatus() { return orderStatus; }
    public void setOrderStatus(String orderStatus) { this.orderStatus = orderStatus; }

    public String getShippingStatus() { return shippingStatus; }
    public void setShippingStatus(String shippingStatus) { this.shippingStatus = shippingStatus; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "OrderEvent{eventId='" + eventId + "', eventType='" + eventType +
               "', orderId=" + orderId + ", username='" + username +
               "', productId=" + productId + ", quantity=" + quantity +
               ", shippingStatus='" + shippingStatus + "'}";
    }
}
