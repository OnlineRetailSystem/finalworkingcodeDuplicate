import React from "react";

export default function OrderSummary({ subtotal, shipping, total, onCheckout }) {
  return (
    <aside className="order-summary">
      <h3>Order Summary</h3>
      <div className="order-row">
        <span>Subtotal:</span>
        <span>₹{subtotal.toFixed(2)}</span>
      </div>
      <div className="order-row">
        <span>Shipping:</span>
        <span>₹{shipping.toFixed(2)}</span>
      </div>
      <div className="order-row order-total">
        <span>Total:</span>
        <span>₹{total.toFixed(2)}</span>
      </div>
      <button className="order-btn" onClick={onCheckout}>
        Proceed to Checkout
      </button>
    </aside>
  );
}