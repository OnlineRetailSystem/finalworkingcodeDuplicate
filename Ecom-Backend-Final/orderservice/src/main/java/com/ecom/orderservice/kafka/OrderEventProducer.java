package com.ecom.orderservice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.ecom.orderservice.events.OrderEvent;
import com.ecom.orderservice.models.Order;

@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private static final String TOPIC_ORDER_PLACED = "ORDER_PLACED";
    private static final String TOPIC_ORDER_STATUS_UPDATED = "ORDER_STATUS_UPDATED";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderPlaced(Order order) {
        OrderEvent event = new OrderEvent(
                "ORDER_PLACED",
                order.getId(),
                order.getUsername(),
                order.getProductId(),
                order.getCategory(),
                order.getQuantity(),
                order.getTotalPrice(),
                order.getOrderStatus(),
                order.getShippingStatus());
        log.info("Publishing ORDER_PLACED event: {}", event);
        kafkaTemplate.send(TOPIC_ORDER_PLACED, event.getEventId(), event);
    }

    public void publishOrderStatusUpdated(Order order) {
        OrderEvent event = new OrderEvent(
                "ORDER_STATUS_UPDATED",
                order.getId(),
                order.getUsername(),
                order.getProductId(),
                order.getCategory(),
                order.getQuantity(),
                order.getTotalPrice(),
                order.getOrderStatus(),
                order.getShippingStatus());
        log.info("Publishing ORDER_STATUS_UPDATED event: {}", event);
        kafkaTemplate.send(TOPIC_ORDER_STATUS_UPDATED, event.getEventId(), event);
    }
}
