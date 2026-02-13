package com.example.stripepayment.controller;

import com.example.stripepayment.kafka.PaymentEventProducer;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.param.PaymentIntentCreateParams;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentEventProducer paymentEventProducer;

    public PaymentController(PaymentEventProducer paymentEventProducer) {
        this.paymentEventProducer = paymentEventProducer;
    }

    @PostMapping("/create")
    public ResponseEntity<Map<String, String>> createPayment(@RequestBody Map<String, Object> data) throws StripeException {
        Object amountObj = data.get("amount");
        Long amount;
        if (amountObj instanceof Number) {
            amount = ((Number) amountObj).longValue();
        } else if (amountObj instanceof String) {
            amount = Long.parseLong((String) amountObj);
        } else {
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid amount"));
        }
        String currency = "usd"; // fixed for now

        PaymentIntentCreateParams params = PaymentIntentCreateParams.builder()
            .setAmount(amount)
            .setCurrency(currency)
            .build();

        PaymentIntent intent = PaymentIntent.create(params);

        // Extract order info from request for Kafka event
        String username = data.get("username") != null ? data.get("username").toString() : "unknown";
        Long productId = data.get("productId") != null ? Long.parseLong(data.get("productId").toString()) : null;
        Integer quantity = data.get("quantity") != null ? Integer.parseInt(data.get("quantity").toString()) : 1;

        // Publish PAYMENT_SUCCESS event to Kafka
        paymentEventProducer.publishPaymentSuccess(
                username, productId, quantity, amount, currency, intent.getId()
        );

        Map<String, String> response = new HashMap<>();
        response.put("clientSecret", intent.getClientSecret());
        response.put("paymentIntentId", intent.getId());
        response.put("status", "PAYMENT_SUCCESS");
        return ResponseEntity.ok(response);
    }
}