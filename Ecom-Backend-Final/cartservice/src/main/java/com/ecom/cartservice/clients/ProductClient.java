package com.ecom.cartservice.clients;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.ecom.cartservice.dto.ProductDTO;

@FeignClient(name = "productservice")
public interface ProductClient {

    @GetMapping("/products/{id}")
    ProductDTO getProductById(@PathVariable Long id);
    

	@PutMapping("/products/{id}/reduce-stock")
	    void reduceStock(@PathVariable Long id, @RequestParam Integer quantity);
	

}
