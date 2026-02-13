package com.ecom.categoryservice.clients;

import com.ecom.categoryservice.dto.ProductDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.List;

@FeignClient(name = "productservice")
public interface ProductClient {

    @GetMapping("/products/category/{category}")
    List<ProductDTO> getProductsByCategory(@PathVariable String category);
}
