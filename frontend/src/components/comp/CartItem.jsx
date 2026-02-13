import React from "react";

export default function CartItem({ name, price, image, qty, onQtyChange, onDelete }) {
  return (
    <div className="cart-item">
      <img src={image} alt={name} className="cart-item-image"/>
      <div className="cart-item-details">
        <div className="cart-item-name">{name}</div>
        <div className="cart-item-price">₹{price}</div>
      </div>
      <div className="cart-item-meta">
        <button className="qty-btn" onClick={() => onQtyChange(qty - 1)} disabled={qty <= 1}>-</button>
        <span style={{margin: "0 10px", fontWeight: 600}}>{qty}</span>
        <button className="qty-btn" onClick={() => onQtyChange(qty + 1)}>+</button>
        <button onClick={onDelete} className="remove-btn" title="Remove">
          ✕
        </button>
      </div>
    </div>
  );
}