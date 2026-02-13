package com.ecom.orderservice.kafka;

import java.time.LocalDateTime;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.ecom.orderservice.models.Order;
import com.ecom.orderservice.repositories.OrderRepository;

@Service
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final OrderRepository orderRepository;
    private final OrderEventProducer orderEventProducer;

    public PaymentEventConsumer(OrderRepository orderRepository, OrderEventProducer orderEventProducer) {
        this.orderRepository = orderRepository;
        this.orderEventProducer = orderEventProducer;
    }

    @KafkaListener(topics = "PAYMENT_SUCCESS", groupId = "order-service-group")
    public void consumePaymentSuccess(Map<String, Object> eventData) {
        try {
            String eventId = (String) eventData.get("eventId");
            log.info("Received PAYMENT_SUCCESS event with eventId: {}", eventId);

            // Idempotency check: prevent duplicate order creation
            if (orderRepository.findBySourceEventId(eventId).isPresent()) {
                log.warn("Duplicate PAYMENT_SUCCESS event detected, eventId: {}. Skipping.", eventId);
                return;
            }

            String username = (String) eventData.get("username");
            Long productId = eventData.get("productId") != null ? Long.valueOf(eventData.get("productId").toString())
                    : null;
            Integer quantity = eventData.get("quantity") != null ? Integer.valueOf(eventData.get("quantity").toString())
                    : 1;

            // Payment Service sends 'amount' (in cents), we store 'totalPrice' (in dollars)
            Long amount = eventData.get("amount") != null ? Long.valueOf(eventData.get("amount").toString()) : 0L;

            // Create order from payment event
            Order order = new Order();
            order.setUsername(username);
            order.setProductId(productId);
            order.setQuantity(quantity);
            order.setTotalPrice(amount / 100.0); // Convert cents to dollars
            order.setOrderStatus("CONFIRMED");
            order.setShippingStatus("PENDING");
            order.setOrderDate(LocalDateTime.now());
            order.setSourceEventId(eventId);

            Order savedOrder = orderRepository.save(order);
            log.info("Order created from PAYMENT_SUCCESS: orderId={}", savedOrder.getId());

            // Publish ORDER_PLACED event
            orderEventProducer.publishOrderPlaced(savedOrder);

        } catch (Exception e) {
            log.error("Error processing PAYMENT_SUCCESS event: {}", e.getMessage(), e);
        }
    }
}
