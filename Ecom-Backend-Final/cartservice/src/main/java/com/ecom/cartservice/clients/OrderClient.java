package com.ecom.cartservice.clients;

import com.ecom.cartservice.dto.OrderRequestDTO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "orderservice")
public interface OrderClient {

    @PostMapping("/orders")
    void placeOrder(@RequestBody OrderRequestDTO orderRequest);
}
