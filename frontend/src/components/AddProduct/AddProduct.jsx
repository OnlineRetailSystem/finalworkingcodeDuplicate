import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import "../AddProduct/AddProduct.css";

export default function AddProduct() {
  const [form, setForm] = useState({
    name: "",
    description: "",
    price: "",
    quantity: "",
    category: "",
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();

  function handleChange(e) {
    setForm({ ...form, [e.target.name]: e.target.value });
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setLoading(true); setError("");
    if (!form.name || !form.price || !form.quantity || !form.category) {
      setError("All fields except description are required.");
      setLoading(false);
      return;
    }
    try {
      const res = await fetch("http://localhost:8082/products", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...form,
          price: Number(form.price),
          quantity: Number(form.quantity),
        }),
      });
      if (!res.ok) throw new Error("Failed to add product.");
      navigate("/admindashboard");
    } catch (err) {
      setError(err.message || "Failed to add product.");
    }
    setLoading(false);
  }

  return (
    <div className="addproduct-bg">
      <form className="addproduct-form" onSubmit={handleSubmit}>
        <h2 className="addproduct-title">Add Product</h2>
        <div className="addproduct-form-group">
          <label>Name</label>
          <input
            className="addproduct-input"
            name="name"
            value={form.name}
            onChange={handleChange}
            type="text"
            placeholder="Product name"
            autoFocus
            required
          />
        </div>
        <div className="addproduct-form-group">
          <label>Description</label>
          <textarea
            className="addproduct-input addproduct-textarea"
            name="description"
            value={form.description}
            onChange={handleChange}
            placeholder="Description"
            rows={3}
          />
        </div>
        <div className="addproduct-form-row">
          <div className="addproduct-form-group">
            <label>Price</label>
            <input
              className="addproduct-input"
              name="price"
              value={form.price}
              onChange={handleChange}
              type="number"
              min="0"
              step="0.01"
              placeholder="Price (₹)"
              required
            />
          </div>
          <div className="addproduct-form-group">
            <label>Quantity</label>
            <input
              className="addproduct-input"
              name="quantity"
              value={form.quantity}
              onChange={handleChange}
              type="number"
              min="0"
              step="1"
              placeholder="Quantity"
              required
            />
          </div>
        </div>
        <div className="addproduct-form-group">
          <label>Category</label>
          <input
            className="addproduct-input"
            name="category"
            value={form.category}
            onChange={handleChange}
            type="text"
            placeholder="Category"
            required
          />
        </div>
        {error && <div className="addproduct-error">{error}</div>}
        <button className="addproduct-btn" type="submit" disabled={loading}>
          {loading ? "Adding..." : "Add Product"}
        </button>
      </form>
    </div>
  );
}