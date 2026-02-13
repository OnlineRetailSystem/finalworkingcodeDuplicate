import React, { useEffect, useState } from "react";
import phoneImg from "../../assets/phone.webp";
import "../OrderHistory/OrderHistory.css";

// Utility to get username
function getUsername() {
  return localStorage.getItem("username");
}

// Utility to add a week to a date string and format nicely
function formatDeliveryDate(orderDate) {
  let d = orderDate ? new Date(orderDate) : new Date();
  d.setDate(d.getDate() + 7);
  // Format: 23rd March 2021
  const day = d.getDate();
  const suffix = (n) => ["th","st","nd","rd"][(n % 10 > 3 || ~~(n % 100 / 10) === 1) ? 0 : n % 10];
  const month = d.toLocaleString('default', { month: 'long' });
  return `${day}${suffix(day)} ${month} ${d.getFullYear()}`;
}

export default function OrderHistory() {
  const [orders, setOrders] = useState([]);
  const [products, setProducts] = useState({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchOrdersAndProducts() {
      const username = getUsername();
      if (!username) return setLoading(false);

      // 1. Fetch all orders from the correct API (8083)
      const orderRes = await fetch("http://localhost:8083/orders");
      const orderData = await orderRes.json();

      // 2. Filter orders by username
      const userOrders = Array.isArray(orderData)
        ? orderData.filter(o => o.username === username)
        : [];
      setOrders(userOrders);

      // 3. Fetch all product details for those orders (for names, etc)
      const productIds = [...new Set(userOrders.map(o => o.productId))];
      if (productIds.length > 0) {
        const prodRes = await fetch("http://localhost:8082/products");
        const prodData = await prodRes.json();
        const map = {};
        (Array.isArray(prodData) ? prodData : []).forEach(prod => { map[prod.id] = prod; });
        setProducts(map);
      }
      setLoading(false);
    }
    fetchOrdersAndProducts();
  }, []);

  if (loading) return <div className="order-history-bg"><div className="order-history-loading">Loading...</div></div>;

  return (
    <div className="order-history-bg">
      <div className="order-history-container">
        <h2 className="order-history-title">Order History</h2>
        <div className="order-history-header-row">
          <span className="oh-col-product">Product</span>
          <span className="oh-col-price">Price</span>
          <span className="oh-col-qty">Qty</span>
          <span className="oh-col-date">Delivery date</span>
        </div>
        <div className="order-history-list">
          {orders.length === 0 ? (
            <div className="order-history-empty">No orders found.</div>
          ) : (
            orders.map(order => {
              const prod = products[order.productId] || {};
              return (
                <div className="oh-list-row" key={order.id || order.orderId}>
                  <div className="oh-prod-col">
                    <img className="oh-prod-img" src={prod.image || phoneImg} alt={prod.name}/>
                    <div>
                      <div className="oh-prod-title">{prod.name || "Product"}</div>
                      <div className="oh-prod-variant">{prod.variant || prod.color || prod.description || ""}</div>
                    </div>
                  </div>
                  <div className="oh-price-col">
                    ₹{Number(order.totalPrice || order.price || prod.price || 0).toLocaleString()}
                  </div>
                  <div className="oh-qty-col">
                    <span>{order.quantity}</span>
                  </div>
                  <div className="oh-date-col">
                    {formatDeliveryDate(order.orderDate || order.date)}
                  </div>
                </div>
              )
            })
          )}
        </div>
      </div>
    </div>
  );
}