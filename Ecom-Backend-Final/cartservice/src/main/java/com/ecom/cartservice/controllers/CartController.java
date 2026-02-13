package com.ecom.cartservice.controllers;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ecom.cartservice.clients.OrderClient;
import com.ecom.cartservice.clients.ProductClient;
import com.ecom.cartservice.dto.OrderRequestDTO;
import com.ecom.cartservice.dto.ProductDTO;
import com.ecom.cartservice.models.CartItem;
import com.ecom.cartservice.repositories.CartRepository;

@RestController
@RequestMapping("/cart")
public class CartController {

    @Autowired
    private CartRepository cartRepository;

   
    @Autowired
    private ProductClient productClient;
    
    @Autowired
    private OrderClient orderClient;

    // Add item to cart
    @PostMapping("/add")
    public ResponseEntity<?> addToCart(@RequestBody CartItem item) {
        ProductDTO product = productClient.getProductById(item.getProductId());

        if (product == null) {
            return ResponseEntity.badRequest().body("Product not found");
        }

        item.setProductName(product.getName());
        item.setPrice(product.getPrice());

        return ResponseEntity.ok(cartRepository.save(item));
    }

    @PostMapping("/checkout/{username}")
    public ResponseEntity<String> placeOrderFromCart(@PathVariable String username) {
        List<CartItem> cartItems = cartRepository.findByUsername(username);

        if (cartItems.isEmpty()) {
            return ResponseEntity.badRequest().body("Cart is empty");
        }

        for (CartItem item : cartItems) {
            // Reduce stock
            productClient.reduceStock(item.getProductId(), item.getQuantity());

            // Create order
            OrderRequestDTO order = new OrderRequestDTO();
            order.setUsername(item.getUsername());
            order.setProductId(item.getProductId());
            order.setQuantity(item.getQuantity());
            order.setTotalPrice(item.getPrice() * item.getQuantity());

            orderClient.placeOrder(order);
        }

        // Clear cart
        cartRepository.deleteByUsername(username);

        return ResponseEntity.ok("Order placed successfully");
    }


    // Get all items in user's cart
    @GetMapping("/{username}")
    public ResponseEntity<List<CartItem>> getCartItems(@PathVariable String username) {
        return ResponseEntity.ok(cartRepository.findByUsername(username));
    }

    // Update quantity of a cart item
    @PutMapping("/{id}")
    public ResponseEntity<CartItem> updateCartItem(@PathVariable Long id, @RequestBody CartItem updatedItem) {
        return cartRepository.findById(id)
                .map(item -> {
                    item.setQuantity(updatedItem.getQuantity());
                    return ResponseEntity.ok(cartRepository.save(item));
                })
                .orElse(ResponseEntity.notFound().build());
    }

    // Remove a specific item from cart
    @DeleteMapping("/{id}")
    public ResponseEntity<Object> removeCartItem(@PathVariable Long id) {
        return cartRepository.findById(id)
                .map(item -> {
                    cartRepository.delete(item);
                    return ResponseEntity.noContent().build();
                })
                .orElse(ResponseEntity.notFound().build());
    }

    // Clear all items from user's cart
    @DeleteMapping("/clear/{username}")
    public ResponseEntity<Void> clearCart(@PathVariable String username) {
        cartRepository.deleteByUsername(username);
        return ResponseEntity.noContent().build();
    }
}
