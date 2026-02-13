package com.ecom.orderservice.controller;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ecom.orderservice.clients.ProductClient;
import com.ecom.orderservice.dto.OrderResponseDTO;
import com.ecom.orderservice.dto.ProductDTO;
import com.ecom.orderservice.kafka.OrderEventProducer;
import com.ecom.orderservice.models.Order;
import com.ecom.orderservice.repositories.OrderRepository;

@RestController
@RequestMapping("/orders")
public class OrderController {

	@Autowired
	private OrderRepository orderRepository;

	@Autowired
	private ProductClient productClient;

	@Autowired
	private OrderEventProducer orderEventProducer;

	private static final List<String> VALID_SHIPPING_STATUSES =
			Arrays.asList("PENDING", "SHIPPED", "DELIVERED", "CANCELLED");

	@PostMapping
	public ResponseEntity<?> placeOrder(@RequestBody Order order) {
		try {
			// Fetch product details
			ProductDTO product = productClient.getProductById(order.getProductId());
			if (product == null) {
				return ResponseEntity.badRequest().body("Product not found");
			}
			if (order.getQuantity() > product.getQuantity()) {
				return ResponseEntity.badRequest().body("Insufficient stock");
			}

			// Stock reduction is handled asynchronously via ORDER_PLACED event in ProductService
			// productClient.reduceStock(order.getProductId(), order.getQuantity());

			// Set order fields
			order.setTotalPrice(product.getPrice() * order.getQuantity());
			order.setOrderDate(LocalDateTime.now());
			order.setCategory(product.getCategory());
			order.setShippingStatus("PENDING");

			// Save order
			Order savedOrder = orderRepository.save(order);

			// Publish ORDER_PLACED event to Kafka
			orderEventProducer.publishOrderPlaced(savedOrder);

			// Build response DTO
			OrderResponseDTO responseDTO = new OrderResponseDTO(
				savedOrder.getId(),
				savedOrder.getUsername(),
				savedOrder.getProductId(),
				savedOrder.getCategory(),
				savedOrder.getQuantity(),
				savedOrder.getTotalPrice(),
				savedOrder.getOrderDate(),
				savedOrder.getOrderStatus(),
				savedOrder.getShippingStatus()
			);

			return ResponseEntity.ok(responseDTO);
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error placing order: " + e.getMessage());
		}
	}

	@GetMapping
	public ResponseEntity<List<Order>> getAllOrders() {
		return ResponseEntity.ok(orderRepository.findAll());
	}

	@GetMapping("/{id}")
	public ResponseEntity<Order> getOrderById(@PathVariable Long id) {
		return orderRepository.findById(id)
				.map(ResponseEntity::ok)
				.orElse(ResponseEntity.notFound().build());
	}

	@DeleteMapping("/{id}")
	public ResponseEntity<Object> deleteOrder(@PathVariable Long id) {
		return orderRepository.findById(id)
				.map(order -> {
					orderRepository.delete(order);
					return ResponseEntity.noContent().build();
				})
				.orElse(ResponseEntity.notFound().build());
	}

	@GetMapping("/countbycategory")
	public ResponseEntity<List<Map<String, Object>>> getOrderCountByCategory() {
		List<Map<String, Object>> counts = orderRepository.countOrdersByCategory();
		return ResponseEntity.ok(counts);
	}

	@GetMapping("/countbystatus")
	public ResponseEntity<List<Map<String, Object>>> getOrderCountByStatus() {
		List<Map<String, Object>> counts = orderRepository.countOrdersByStatus();
		return ResponseEntity.ok(counts);
	}

	@PutMapping("/{id}")
	public ResponseEntity<?> updateOrder(@PathVariable Long id, @RequestBody Order updatedOrder) {
		try {
			return orderRepository.findById(id).map(existingOrder -> {
				if (updatedOrder.getQuantity() != null) {
					existingOrder.setQuantity(updatedOrder.getQuantity());
				}
				if (updatedOrder.getOrderStatus() != null) {
					existingOrder.setOrderStatus(updatedOrder.getOrderStatus());
				}
				if (updatedOrder.getQuantity() != null || (updatedOrder.getProductId() != null)) {
					Long productIdToUse = updatedOrder.getProductId() != null ? updatedOrder.getProductId() : existingOrder.getProductId();
					ProductDTO product = productClient.getProductById(productIdToUse);
					if (product == null) {
						return ResponseEntity.badRequest().body("Product not found");
					}
					int quantity = updatedOrder.getQuantity() != null ? updatedOrder.getQuantity() : existingOrder.getQuantity();
					if (quantity > product.getQuantity()) {
						return ResponseEntity.badRequest().body("Insufficient stock");
					}
					productClient.reduceStock(productIdToUse, quantity - existingOrder.getQuantity());

					existingOrder.setTotalPrice(product.getPrice() * quantity);
					existingOrder.setProductId(productIdToUse);
					existingOrder.setCategory(product.getCategory());
					existingOrder.setOrderDate(LocalDateTime.now());
				}
				Order savedOrder = orderRepository.save(existingOrder);
				return ResponseEntity.ok(savedOrder);
			}).orElse(ResponseEntity.notFound().build());
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error updating order");
		}
	}

	/**
	 * Update Shipping Status - ADMIN only
	 * Valid statuses: PENDING, SHIPPED, DELIVERED, CANCELLED
	 */
	@PutMapping("/{id}/shipping-status")
	public ResponseEntity<?> updateShippingStatus(
			@PathVariable Long id,
			@RequestParam String status) {
		try {
			if (!VALID_SHIPPING_STATUSES.contains(status.toUpperCase())) {
				return ResponseEntity.badRequest()
						.body("Invalid shipping status. Valid values: " + VALID_SHIPPING_STATUSES);
			}

			return orderRepository.findById(id).map(order -> {
				order.setShippingStatus(status.toUpperCase());
				Order savedOrder = orderRepository.save(order);

				// Publish ORDER_STATUS_UPDATED event
				orderEventProducer.publishOrderStatusUpdated(savedOrder);

				return ResponseEntity.ok(savedOrder);
			}).orElse(ResponseEntity.notFound().build());
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body("Error updating shipping status: " + e.getMessage());
		}
	}
}
