package com.ecom.cartservice.repositories;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;

import com.ecom.cartservice.models.CartItem;

import jakarta.transaction.Transactional;

public interface CartRepository extends JpaRepository<CartItem, Long> {
    List<CartItem> findByUsername(String username);
    
    @Transactional
    @Modifying
    void deleteByUsername(String username);
}
