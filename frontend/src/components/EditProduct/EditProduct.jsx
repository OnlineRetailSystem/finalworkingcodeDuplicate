import React, { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import "../EditProduct/EditProduct.css";

export default function EditProduct() {
  const { productId } = useParams(); // <-- get productId from /edit-product/:productId
  const [form, setForm] = useState({
    name: "",
    description: "",
    price: "",
    quantity: "",
    category: ""
  });
  const [loading, setLoading] = useState(true);
  const [saveLoading, setSaveLoading] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();

  useEffect(() => {
    async function fetchProduct() {
      setLoading(true);
      try {
        const res = await fetch(`http://localhost:8082/products/${productId}`, {
          method: "GET",
          mode: "cors"
        });
        if (!res.ok) throw new Error("Product not found");
        const prod = await res.json();
        setForm({
          name: prod.name || "",
          description: prod.description || "",
          price: prod.price?.toString() || "",
          quantity: prod.quantity?.toString() || "",
          category: prod.category || ""
        });
        setError("");
      } catch (err) {
        setError("Failed to load product for editing.");
      }
      setLoading(false);
    }
    fetchProduct();
  }, [productId]);

  function handleChange(e) {
    setForm({ ...form, [e.target.name]: e.target.value });
  }

  async function handleSave(e) {
    e.preventDefault();
    setSaveLoading(true);
    setError("");
    try {
      if (!form.name || !form.price || !form.quantity || !form.category)
        throw new Error("All fields except description are required.");
      const res = await fetch(`http://localhost:8082/products/${productId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...form,
          price: Number(form.price),
          quantity: Number(form.quantity)
        })
      });
      if (!res.ok) throw new Error("Failed to update product.");
      navigate("/admindashboard");
    } catch (err) {
      setError(err.message || "Failed to update product.");
    }
    setSaveLoading(false);
  }

  if (loading)
    return <div className="addproduct-bg"><div className="pdp-loading">Loading...</div></div>;
  if (error)
    return <div className="addproduct-bg"><div className="addproduct-error">{error}</div></div>;

  return (
    <div className="addproduct-bg">
      <form className="addproduct-form" onSubmit={handleSave}>
        <h2 className="addproduct-title">Edit Product</h2>
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
        <button className="addproduct-btn" type="submit" disabled={saveLoading}>
          {saveLoading ? "Saving..." : "Save"}
        </button>
      </form>
    </div>
  );
}