package com.ecom.productservice.repositories;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.ecom.productservice.models.Product;

public interface ProductRepository extends JpaRepository<Product, Long> {

	List<Product> findByCategory(String category);
	
	@Query("SELECT p.category, COUNT(p) FROM Product p GROUP BY p.category")
	List<Object[]> countProductsByCategory();
}
