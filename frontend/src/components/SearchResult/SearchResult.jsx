import React, { useEffect, useState } from "react";
import phoneImg from "../../assets/phone.webp";
import { useNavigate } from "react-router-dom";
import "./SearchResult.css";

export default function SearchResult({ searchString }) {
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  useEffect(() => {
    async function fetchProducts() {
      try {
        const res = await fetch("http://localhost:8082/products");
        const data = await res.json();
        setProducts(Array.isArray(data) ? data : [data]);
      } finally {
        setLoading(false);
      }
    }
    fetchProducts();
  }, []);

  // Filter by name, desc, category case-insensitive
  const filtered = products.filter((prod) => {
    const term = (searchString || "").trim().toLowerCase();
    return (
      term &&
      (
        (prod.name && prod.name.toLowerCase().includes(term)) ||
        (prod.description && prod.description.toLowerCase().includes(term)) ||
        (prod.category && prod.category.toLowerCase().includes(term))
      )
    );
  });

  if (loading) return <div className="sr-loading">Loading...</div>;
  if (!searchString || filtered.length === 0)
    return <div className="sr-empty">No results found.</div>;

  return (
    <div className="sr-bg">
      <div className="sr-result-list">
        {filtered.map((product) => (
          <div
            key={product.id}
            className="sr-result-card"
            onClick={() => navigate(`/product/${product.id}`)}
            style={{ cursor: "pointer" }}
          >
            <div className="sr-result-imgside">
              <img src={phoneImg} alt={product.name} className="sr-result-img" />
              <label className="sr-checkbox-label" onClick={e => e.stopPropagation()}>
                <input type="checkbox" className="sr-checkbox" />
                Add to Compare
              </label>
            </div>
            <div className="sr-result-details">
              <div className="sr-title-row">
                <div className="sr-title-wrap">
                  <span className="sr-badge-rating">4.6 ★</span>
                  <span className="sr-title">{product.name}</span>
                </div>
                <div className="sr-price-block">
                  <span className="sr-price">₹{Number(product.price).toLocaleString()}</span>
                  <span className="sr-badge-assured">Assured</span>
                  <span className="sr-bank-offer">Upto ₹40,400 Off on Exchange<br /><span className="bank-offer-green">Bank Offer</span></span>
                </div>
              </div>
              <ul className="sr-features">
                <li>12 GB RAM | 256 GB ROM</li>
                <li>6.2 inch Full HD+ Display</li>
                <li>50MP + 10MP + 12MP | 12MP Front Camera</li>
                <li>4000 mAh Battery</li>
                <li>8 Core Processor</li>
                <li>1 Year Manufacturer Warranty for Device & 6 Months for In-Box Accessories</li>
              </ul>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}