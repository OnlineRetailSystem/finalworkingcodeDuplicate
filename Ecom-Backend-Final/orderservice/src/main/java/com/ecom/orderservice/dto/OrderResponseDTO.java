package com.ecom.orderservice.dto;

import java.time.LocalDateTime;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class OrderResponseDTO {
	public OrderResponseDTO() {}

	public OrderResponseDTO(Long id, String username, Long productId, String category, Integer quantity,
			Double totalPrice, LocalDateTime orderDate, String orderStatus, String shippingStatus) {
		super();
		this.id = id;
		this.username = username;
		this.productId = productId;
		this.category = category;
		this.quantity = quantity;
		this.totalPrice = totalPrice;
		this.orderDate = orderDate;
		this.orderStatus = orderStatus;
		this.shippingStatus = shippingStatus;
	}

	private Long id;
	private String username;
	private Long productId;
	private String category;
	private Integer quantity;
	private Double totalPrice;
	private LocalDateTime orderDate;
	private String orderStatus;
	private String shippingStatus;
}
