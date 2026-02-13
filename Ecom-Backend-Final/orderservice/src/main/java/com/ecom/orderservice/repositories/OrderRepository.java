package com.ecom.orderservice.repositories;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.ecom.orderservice.models.Order;

public interface OrderRepository extends JpaRepository<Order, Long> {
	List<Order> findByCategory(String category);

	Optional<Order> findBySourceEventId(String sourceEventId);

	@Query("SELECT o.category AS category, COUNT(o) AS orderCount FROM Order o GROUP BY o.category")
	List<Map<String, Object>> countOrdersByCategory();

	@Query("SELECT o.orderStatus AS status, COUNT(o) AS count FROM Order o GROUP BY o.orderStatus")
	List<Map<String, Object>> countOrdersByStatus();
}
