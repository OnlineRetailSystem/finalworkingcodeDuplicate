package com.ecom.productservice.kafka;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ecom.productservice.models.ProcessedEvent;
import com.ecom.productservice.models.Product;
import com.ecom.productservice.repositories.ProcessedEventRepository;
import com.ecom.productservice.repositories.ProductRepository;

@Service
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private static final int LOW_STOCK_THRESHOLD = 5;

    private final ProductRepository productRepository;
    private final InventoryEventProducer inventoryEventProducer;
    private final ProcessedEventRepository processedEventRepository;

    public OrderEventConsumer(ProductRepository productRepository,
            InventoryEventProducer inventoryEventProducer,
            ProcessedEventRepository processedEventRepository) {
        this.productRepository = productRepository;
        this.inventoryEventProducer = inventoryEventProducer;
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaListener(topics = "ORDER_PLACED", groupId = "inventory-service-group")
    @Transactional
    public void consumeOrderPlaced(Map<String, Object> eventData) {
        String eventId = (String) eventData.get("eventId");

        // Idempotency check: prevent double stock reduction
        if (eventId != null && processedEventRepository.existsById(eventId)) {
            log.info("Duplicate ORDER_PLACED event detected: {}. Skipping.", eventId);
            return;
        }

        log.info("Received ORDER_PLACED event with eventId: {}", eventId);

        Long productId = eventData.get("productId") != null ? Long.valueOf(eventData.get("productId").toString())
                : null;
        Integer quantity = eventData.get("quantity") != null ? Integer.valueOf(eventData.get("quantity").toString())
                : 0;

        if (productId == null) {
            log.warn("ORDER_PLACED event missing productId, skipping. eventId: {}", eventId);
            return;
        }

        Optional<Product> productOpt = productRepository.findById(productId);
        if (productOpt.isEmpty()) {
            log.error("Product not found for productId: {} in ORDER_PLACED event: {}", productId, eventId);
            return;
        }

        Product product = productOpt.get();
        int currentStock = product.getQuantity();

        if (currentStock < quantity) {
            log.error("Insufficient stock for productId: {}. Available: {}, Requested: {}",
                    productId, currentStock, quantity);
            return;
        }

        // Reduce stock transactionally
        product.setQuantity(currentStock - quantity);
        productRepository.save(product);
        log.info("Stock reduced for productId: {}. New stock: {}", productId, product.getQuantity());

        // Check if stock is low -> publish alert
        if (product.getQuantity() < LOW_STOCK_THRESHOLD) {
            log.warn("LOW STOCK ALERT: productId={}, name={}, stock={}",
                    productId, product.getName(), product.getQuantity());
            inventoryEventProducer.publishLowStockAlert(
                    productId,
                    product.getName(),
                    product.getQuantity(),
                    LOW_STOCK_THRESHOLD);
        }

        // Mark event as processed (Idempotency)
        if (eventId != null) {
            processedEventRepository.save(new ProcessedEvent(eventId, "ORDER_PLACED", LocalDateTime.now()));
        }
    }
}
