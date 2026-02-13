import React, { useEffect, useState } from "react";
import { FiEdit2, FiTrash2, FiPlus } from "react-icons/fi";
import { useNavigate } from "react-router-dom";
import phoneImg from "../../assets/phone.webp";
import "../AdminDashboard/AdminDashboard.css";

function groupByCategory(products) {
  return products.reduce((acc, prod) => {
    (acc[prod.category] = acc[prod.category] || []).push(prod);
    return acc;
  }, {});
}

export default function AdminDashboard() {
  const [groupedProducts, setGroupedProducts] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [refresh, setRefresh] = useState(0);
  const navigate = useNavigate();

  useEffect(() => {
    async function fetchProducts() {
      try {
        const res = await fetch("http://localhost:8082/products", {
          method: "GET",
          mode: "cors"
        });
        if (!res.ok) throw new Error("Failed to load products");
        const data = await res.json();
        setGroupedProducts(groupByCategory(Array.isArray(data) ? data : [data]));
      } catch {
        setError("Failed to load products.");
      } finally {
        setLoading(false);
      }
    }
    fetchProducts();
  }, [refresh]);

  async function handleDelete(productId) {
    if (!window.confirm("Delete this product?")) return;
    await fetch(`http://localhost:8082/products/${productId}`, { method: "DELETE" });
    setRefresh(r => r + 1);
  }

  if (loading) return <div className="pcs-center-text">Loading...</div>;
  if (error) return <div className="pcs-center-text pcs-error">{error}</div>;

  return (
    <div className="pcs-bg">
      {Object.entries(groupedProducts).map(([category, products]) => (
        <div className="pcs-row-container" key={category}>
          <div className="pcs-category-title-row">
            <span className="pcs-category-title">{category}</span>
            <button
              className="pcs-card-btn pcs-add-product-btn"
              type="button"
              onClick={() => navigate("/add-product")}
            >
              <FiPlus style={{ marginRight: 7, fontSize: "1.11em" }} />
              Add Product
            </button>
          </div>
          <div className="pcs-product-list-grid">
            {products.map(product => (
              <div key={product.id} className="pcs-card-outer">
                <div
                  className="pcs-card"
                  tabIndex={0}
                  title={product.name}
                  onClick={e => {
                    if (
                      !e.target.classList.contains("pcs-edit-btn") &&
                      !e.target.classList.contains("pcs-delete-btn") &&
                      !e.target.closest(".pcs-edit-btn") &&
                      !e.target.closest(".pcs-delete-btn")
                    ) {
                      navigate(`/product-details/${product.id}`);
                    }
                  }}
                >
                  <img src={phoneImg} alt={product.name} className="pcs-card-img" />
                  <div className="pcs-card-name">
                    {product.name.length > 24 ? product.name.slice(0, 22) + "..." : product.name}
                  </div>
                  <div className="pcs-card-price">
                    ₹{parseInt(product.price)}
                  </div>
                  <div className="pcs-btn-row">
                    <button
                      className="pcs-card-btn pcs-edit-btn"
                      type="button"
                      onClick={e => {
                        e.stopPropagation();
                        navigate(`/edit-product/${product.id}`);
                      }}
                    >
                      <FiEdit2 style={{ marginRight: 6 }} />
                      Edit
                    </button>
                    <button
                      className="pcs-card-btn pcs-delete-btn"
                      type="button"
                      onClick={e => {
                        e.stopPropagation();
                        handleDelete(product.id);
                      }}
                    >
                      <FiTrash2 style={{ marginRight: 6 }} />
                      Delete
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}