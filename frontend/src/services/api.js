// src/services/api.js
import { ORDER_SERVICE_URL, PRODUCT_SERVICE_URL } from '../config/api.config';

export const ORDER_SERVICE_BASE_URL = ORDER_SERVICE_URL;
export const PRODUCT_SERVICE_BASE_URL = PRODUCT_SERVICE_URL;

export const endpoints = {
  ordersCountByStatus: `${ORDER_SERVICE_BASE_URL}/orders/countbystatus`, // -> [{ "status": "Pending", "count": 1 }]
  productsCountByCategory: `${PRODUCT_SERVICE_BASE_URL}/products/countbycategory`, // -> [["Laptop", 3], ...]
};
