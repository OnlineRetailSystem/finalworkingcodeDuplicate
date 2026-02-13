import React, { useEffect, useState } from "react";
import { FiMinus, FiPlus, FiTrash2 } from "react-icons/fi";
import phoneImg from "../../assets/phone.webp";
import "../Cart/Cart.css";
import { useNavigate } from "react-router-dom";
import Swal from 'sweetalert2'; // npm install sweetalert2

function getUsername() {
  return localStorage.getItem("username");
}

export default function Cart() {
  const [cartItems, setCartItems] = useState([]);
  const [products, setProducts] = useState({});
  const [loading, setLoading] = useState(true);
  const [summary, setSummary] = useState({ total: 0, subtotal: 0, count: 0 });
  const username = getUsername();
  const navigate = useNavigate();

  useEffect(() => {
    async function fetchCart() {
      if (!username) return setLoading(false);
      setLoading(true);
      try {
        const res = await fetch(`http://localhost:8084/cart/${username}`);
        const data = await res.json();
        setCartItems(Array.isArray(data) ? data : []);
      } finally {
        setLoading(false);
      }
    }
    fetchCart();
  }, [username]);

  useEffect(() => {
    async function fetchProductsForCart() {
      const ids = Array.from(new Set(cartItems.map(item => item.productId)));
      if (ids.length === 0) return setProducts({});
      const res = await fetch("http://localhost:8082/products");
      const data = await res.json();
      const map = {};
      (Array.isArray(data) ? data : []).forEach(prod => { map[prod.id] = prod; });
      setProducts(map);
    }
    fetchProductsForCart();
  }, [cartItems]);

  useEffect(() => {
    let total = 0, subtotal = 0, count = cartItems.length;
    cartItems.forEach(item => {
      const p = products[item.productId];
      if (p) {
        total += p.price * item.quantity;
        subtotal += p.price * item.quantity;
      }
    });
    setSummary({ total, subtotal, count });
  }, [cartItems, products]);

  async function updateQty(item, delta) {
    const newQty = item.quantity + delta;
    if (item.quantity === 1 && delta === -1) {
      return removeItem(item.id);
    }
    if (newQty < 1) return;
    await fetch(`http://localhost:8084/cart/${item.id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...item, quantity: newQty })
    });
    reloadCart();
  }
  async function removeItem(cartItemId) {
    await fetch(`http://localhost:8084/cart/${cartItemId}`, { method: "DELETE" });
    reloadCart();
  }
  async function reloadCart() {
    const res = await fetch(`http://localhost:8084/cart/${username}`);
    const data = await res.json();
    setCartItems(Array.isArray(data) ? data : []);
  }
  async function handleClearCart() {
    await fetch(`http://localhost:8084/cart/clear/${username}`, { method: "DELETE" });
    setCartItems([]);
  }
  const deliveryCost = summary.count === 0 ? 0 : 14.0;
  const discount = 0;
  const grandTotal = summary.subtotal + deliveryCost - discount;
  if (!username)
    return (
      <div className="cart-bg">
        <div className="cart-empty-card">
          Please <a href="/usersign">sign in</a> to view your cart.
        </div>
      </div>
    );
  if (loading)
    return (
      <div className="cart-bg">
        <div className="cart-loading">Loading cart...</div>
      </div>
    );

  function handleCheckout() {
    if (grandTotal < 1) {
      Swal.fire({
        icon: "warning",
        title: "Add something to the cart!",
        text: "You cannot checkout with an empty cart or a zero amount.",
        confirmButtonColor: "#1976d2",
      });
      return;
    }
    navigate('/checkout', { state: { amount: grandTotal * 100 } });
  }

  return (
    <div className="cart-bg">
      <div className="cart-main-container">
        {/* Cart table area */}
        <div className="cart-table-card">
          <div className="cart-table-title-row">
            <span className="cart-title">Shopping cart</span>
            <span className="cart-title-right">({String(summary.count).padStart(2,"0")} Items)</span>
            {summary.count > 0 && (
              <button className="cart-clear-btn" onClick={handleClearCart}>
                Clear Cart
              </button>
            )}
          </div>
          <table className="cart-table">
            <thead>
              <tr>
                <th className="cart-th-left">Product</th>
                <th>Price</th>
                <th>Quantity</th>
                <th>Remove</th>
              </tr>
            </thead>
            <tbody>
              {cartItems.map(item => {
                const product = products[item.productId];
                if (!product) return null;
                return (
                  <tr key={item.id} className="cart-tr">
                    <td className="cart-td-left">
                      <div className="cart-prod-cell">
                        <img className="cart-prod-img" src={phoneImg} alt={product.name} />
                        <div>
                          <div className="cart-prod-title">{product.name}</div>
                          <div className="cart-prod-desc">{product.description}</div>
                        </div>
                      </div>
                    </td>
                    <td className="cart-td-text">₹{product.price}</td>
                    <td>
                      <div className="cart-qty-row">
                        <button
                          className="cart-qty-btn"
                          onClick={() => updateQty(item, -1)}
                          disabled={item.quantity < 1}
                        ><FiMinus /></button>
                        <span className="cart-qty-num">{item.quantity}</span>
                        <button
                          className="cart-qty-btn"
                          onClick={() => updateQty(item, +1)}
                        ><FiPlus /></button>
                      </div>
                    </td>
                    <td>
                      <button
                        className="cart-trash-btn"
                        onClick={() => removeItem(item.id)}
                      >
                        <FiTrash2 />
                      </button>
                    </td>
                  </tr>
                )
              })}
              {cartItems.length === 0 && (
                <tr>
                  <td colSpan={4} className="cart-empty-message">Cart is empty.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        {/* Sidebar summary */}
        <div className="cart-summary-card">
          <div className="cart-summary-title">Total</div>
          <div className="cart-summary-row">
            <span>Amount</span>
            <span>₹{summary.subtotal.toLocaleString()}</span>
          </div>
          <div className="cart-summary-row">
            <span>Delivery</span>
            <span>₹{deliveryCost.toLocaleString()}</span>
          </div>
          <div className="cart-summary-row">
            <span>Discount</span>
            <span>-₹{discount.toLocaleString()}</span>
          </div>
          <div className="cart-summary-row cart-summary-bold">
            <span>Total</span>
            <span>₹{grandTotal.toLocaleString()}</span>
          </div>
          <button
            className="cart-summary-btn"
            onClick={handleCheckout}
          >
            Checkout
          </button>
          <div className="cart-summary-paylogo">
            <span><b>We Accept:</b></span>
            <span>
              <img src="https://upload.wikimedia.org/wikipedia/commons/4/41/Visa_Logo.png" alt="Visa" height="16" />
              <img src="https://upload.wikimedia.org/wikipedia/commons/0/04/Mastercard-logo.png" alt="MC" height="16" />
              <img
  src="https://upload.wikimedia.org/wikipedia/commons/b/b5/PayPal.svg"
  alt="PayPal"
  height="16"
/>
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}