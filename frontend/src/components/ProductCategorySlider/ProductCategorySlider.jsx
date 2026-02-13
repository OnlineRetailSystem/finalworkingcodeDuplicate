import React, { useEffect, useState, useRef } from "react";
import { FiChevronRight, FiShoppingCart, FiMinus, FiPlus } from "react-icons/fi";
import { useNavigate } from "react-router-dom";
import phoneImg from "../../assets/phone.webp";
import "./ProductCategorySlider.css";

function groupByCategory(products) {
  return products.reduce((acc, prod) => {
    (acc[prod.category] = acc[prod.category] || []).push(prod);
    return acc;
  }, {});
}

function getUsername() {
  return localStorage.getItem("username");
}

export default function ProductCategorySliders() {
  const [groupedProducts, setGroupedProducts] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
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
  }, []);

  if (loading) return <div className="pcs-center-text">Loading...</div>;
  if (error) return <div className="pcs-center-text pcs-error">{error}</div>;

  return (
    <div className="pcs-bg">
      {Object.entries(groupedProducts).map(([category, products]) => (
        <ProductSliderRow key={category} category={category} products={products} />
      ))}
    </div>
  );
}

function ProductSliderRow({ category, products }) {
  const sliderRef = useRef(null);
  const navigate = useNavigate();
  const [cart, setCart] = useState({}); // { productId: { id, productId, quantity, ... }, ... }
  const [cartLoading, setCartLoading] = useState(false);

  useEffect(() => {
    const username = getUsername();
    async function fetchCart() {
      if (!username) return setCart({});
      try {
        setCartLoading(true);
        const res = await fetch(`http://localhost:8084/cart/${username}`, { method: "GET" });
        if (!res.ok) { setCart({}); return; }
        const data = await res.json();
        setCart(Array.isArray(data)
          ? Object.fromEntries(data.map(item => [item.productId, item]))
          : {});
      } finally {
        setCartLoading(false);
      }
    }
    fetchCart();
  }, []);

  async function handleAddToCart(productId) {
    const username = getUsername();
    if (!username) {
      navigate("/usersign");
      return;
    }
    setCartLoading(true);
    try {
      const res = await fetch("http://localhost:8084/cart/add", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, productId, quantity: 1 })
      });
      if (res.ok) {
        const resp = await fetch(`http://localhost:8084/cart/${username}`);
        const data = await resp.json();
        setCart(Array.isArray(data)
          ? Object.fromEntries(data.map(item => [item.productId, item]))
          : {});
      }
    } finally {
      setCartLoading(false);
    }
  }

  async function handleQtyUpdate(cartItem, delta) {
    if (!cartItem) return;
    const updatedQty = cartItem.quantity + delta;
    if (cartItem.quantity === 1 && delta === -1) {
      // Remove from cart, revert to add button
      setCartLoading(true);
      try {
        await fetch(`http://localhost:8084/cart/${cartItem.id}`, { method: "DELETE" });
        const username = getUsername();
        const resp = await fetch(`http://localhost:8084/cart/${username}`);
        const data = await resp.json();
        setCart(Array.isArray(data)
          ? Object.fromEntries(data.map(item => [item.productId, item]))
          : {});
      } finally {
        setCartLoading(false);
      }
      return;
    }
    if (updatedQty < 1) return;
    setCartLoading(true);
    try {
      await fetch(`http://localhost:8084/cart/${cartItem.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...cartItem, quantity: updatedQty })
      });
      const username = getUsername();
      const resp = await fetch(`http://localhost:8084/cart/${username}`);
      const data = await resp.json();
      setCart(Array.isArray(data)
        ? Object.fromEntries(data.map(item => [item.productId, item]))
        : {});
    } finally {
      setCartLoading(false);
    }
  }

  const scroll = (sign) => {
    if (sliderRef.current) {
      const card = sliderRef.current.querySelector(".pcs-card-outer");
      const pixel = card ? card.offsetWidth + 16 : 210;
      sliderRef.current.scrollBy({
        left: sign * pixel * (window.innerWidth < 600 ? 1 : 3),
        behavior: "smooth"
      });
    }
  };

  return (
    <div className="pcs-row-container">
      <div className="pcs-category-title">{category}</div>
      <div className="pcs-slider-wrap">
        <div ref={sliderRef} className="pcs-slider">
          {products.map(product => {
            const cartItem = cart[product.id];
            return (
              <div key={product.id} className="pcs-card-outer">
                <div
                  onClick={() => navigate(`/product/${product.id}`)}
                  className="pcs-card"
                  tabIndex={0}
                  title={product.name}
                >
                  <img src={phoneImg} alt={product.name} className="pcs-card-img" />
                  <div className="pcs-card-name">
                    {product.name.length > 24 ? product.name.slice(0, 22) + "..." : product.name}
                  </div>
                  <div className="pcs-card-price">
                    ₹{parseInt(product.price)}
                  </div>
                  {cartItem ? (
                    <div className="pcs-qty-row">
                      <button
                        className="pcs-qty-btn"
                        disabled={cartLoading}
                        onClick={e => { e.stopPropagation(); handleQtyUpdate(cartItem, -1); }}
                        aria-label="Decrease quantity"
                      >
                        <FiMinus />
                      </button>
                      <span className="pcs-qty-num">{cartItem.quantity}</span>
                      <button
                        className="pcs-qty-btn"
                        disabled={cartLoading}
                        onClick={e => { e.stopPropagation(); handleQtyUpdate(cartItem, +1); }}
                        aria-label="Increase quantity"
                      >
                        <FiPlus />
                      </button>
                    </div>
                  ) : (
                    <button
                      className="pcs-card-btn"
                      onClick={e => { e.stopPropagation(); handleAddToCart(product.id); }}
                      disabled={cartLoading}
                    >
                      <FiShoppingCart style={{ marginRight: "4px" }} /> Add to Cart
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>
        <button
          className="pcs-arrow-btn pcs-arrow-right"
          onClick={() => scroll(1)}
          aria-label="Scroll right"
        >
          <FiChevronRight size={26} />
        </button>
      </div>
    </div>
  );
}