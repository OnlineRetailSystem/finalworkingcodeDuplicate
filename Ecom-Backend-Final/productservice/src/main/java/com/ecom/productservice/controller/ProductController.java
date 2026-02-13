package com.ecom.productservice.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ecom.productservice.models.Product;
import com.ecom.productservice.repositories.ProductRepository;

@RestController
@RequestMapping("/products")
public class ProductController {

    @Autowired
    private ProductRepository productRepository;

    // Create
    @PostMapping
    public ResponseEntity<Product> addProduct(@RequestBody Product product) {
        Product saved = productRepository.save(product);
        return ResponseEntity.status(HttpStatus.CREATED).body(saved); // 201 Created
    }

    // View all
    @GetMapping
    public ResponseEntity<List<Product>> getAllProducts() {
        return ResponseEntity.ok(productRepository.findAll());
    }

    // ✅ Batch fetch for Category Service: /products?ids=1,2,3
    @GetMapping(params = "ids")
    public ResponseEntity<List<Product>> getProductsByIds(@RequestParam List<Long> ids) {
        return ResponseEntity.ok(productRepository.findAllById(ids));
    }

    // View by id
    @GetMapping("/{id}")
    public ResponseEntity<Product> getProductById(@PathVariable Long id) {
        return productRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // Update
    @PutMapping("/{id}")
    public ResponseEntity<Product> updateProduct(@PathVariable Long id, @RequestBody Product updatedProduct) {
        return productRepository.findById(id).map(product -> {
            if (updatedProduct.getName() != null) {
                product.setName(updatedProduct.getName());
            }
            if (updatedProduct.getDescription() != null) {
                product.setDescription(updatedProduct.getDescription());
            }
            if (updatedProduct.getPrice() != null) {
                product.setPrice(updatedProduct.getPrice());
            }
            if (updatedProduct.getQuantity() != null) {
                product.setQuantity(updatedProduct.getQuantity());
            }
            if (updatedProduct.getCategory() != null) {
                product.setCategory(updatedProduct.getCategory());
            }
            return ResponseEntity.ok(productRepository.save(product));
        }).orElse(ResponseEntity.notFound().build());
    }

    // Reduce stock
    @PutMapping("/{id}/reduce-stock")
    public ResponseEntity<?> reduceStock(@PathVariable Long id, @RequestParam Integer quantity) {
        return productRepository.findById(id).map(product -> {
            if (product.getQuantity() < quantity) {
                return ResponseEntity.badRequest().body("Insufficient stock");
            }
            product.setQuantity(product.getQuantity() - quantity);
            return ResponseEntity.ok(productRepository.save(product));
        }).orElse(ResponseEntity.notFound().build());
    }

    // Delete
    @DeleteMapping("/{id}")
    public ResponseEntity<Object> deleteProduct(@PathVariable Long id) {
        return productRepository.findById(id).map(product -> {
            productRepository.delete(product);
            return ResponseEntity.noContent().build();
        }).orElse(ResponseEntity.notFound().build());
    }
    
    @GetMapping("/category/{category}")
    public List<Product> getProductsByCategory(@PathVariable String category) {
        return productRepository.findByCategory(category);
    }
    
    @GetMapping("/countbycategory")
    public ResponseEntity<List<Object[]>> getProductCountPerCategory() {
        List<Object[]> counts = productRepository.countProductsByCategory();
        return ResponseEntity.ok(counts);
    }

}
