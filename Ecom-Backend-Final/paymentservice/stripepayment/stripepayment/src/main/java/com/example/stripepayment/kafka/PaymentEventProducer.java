package com.example.stripepayment.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.stripepayment.events.PaymentEvent;

@Service
public class PaymentEventProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventProducer.class);
    private static final String TOPIC_PAYMENT_SUCCESS = "PAYMENT_SUCCESS";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentSuccess(String username, Long productId, Integer quantity,
            Long amountPaid, String currency, String paymentIntentId) {
        PaymentEvent event = new PaymentEvent(username, productId, quantity, amountPaid, currency, paymentIntentId);
        log.info("Publishing PAYMENT_SUCCESS event: {}", event);
        kafkaTemplate.send(TOPIC_PAYMENT_SUCCESS, event.getEventId(), event);
    }
}
