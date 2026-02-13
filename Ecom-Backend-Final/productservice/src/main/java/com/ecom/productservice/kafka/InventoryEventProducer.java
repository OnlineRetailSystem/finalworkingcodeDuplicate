package com.ecom.productservice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.ecom.productservice.events.LowStockEvent;

@Service
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);
    private static final String TOPIC_LOW_STOCK_ALERT = "LOW_STOCK_ALERT";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishLowStockAlert(Long productId, String productName, Integer currentStock, Integer threshold) {
        LowStockEvent event = new LowStockEvent(productId, productName, currentStock, threshold);
        log.info("Publishing LOW_STOCK_ALERT event: {}", event);
        kafkaTemplate.send(TOPIC_LOW_STOCK_ALERT, event.getEventId(), event);
    }
}
