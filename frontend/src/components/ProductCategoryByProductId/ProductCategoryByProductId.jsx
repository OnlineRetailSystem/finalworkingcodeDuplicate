import React, { useEffect, useState } from "react";
import { FiShoppingCart, FiPlus, FiMinus } from "react-icons/fi";
import { useNavigate } from "react-router-dom";
import phoneImg from "../../assets/phone.webp";
import "../ProductCategorySlider/ProductCategorySlider.css";

function getUsername() {
  return localStorage.getItem("username");
}

export default function ProductCategoryByProductId({ id }) {
  const [categoryProducts, setCategoryProducts] = useState([]);
  const [categoryName, setCategoryName] = useState("");
  const [cart, setCart] = useState({});
  const [cartLoading, setCartLoading] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const navigate = useNavigate();

  // Fetch products & set category products
  useEffect(() => {
    async function fetchProducts() {
      try {
        const res = await fetch("http://localhost:8082/products", {
          method: "GET",
          mode: "cors"
        });
        if (!res.ok) throw new Error("Failed to load products");
        const data = await res.json();
        const arr = Array.isArray(data) ? data : [data];
        const selectedProduct = arr.find(p => String(p.id) === String(id));
        if (!selectedProduct) throw new Error("Product not found");
        setCategoryName(selectedProduct.category);
        setCategoryProducts(arr.filter(p => String(p.category) === String(selectedProduct.category)));
      } catch {
        setError("Failed to load products.");
      } finally {
        setLoading(false);
      }
    }
    fetchProducts();
  }, [id]);

  // Fetch cart for logged-in user
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

  // Add product to cart
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

  // Update (±) quantity in cart (remove if quantity=1 and - pressed)
  async function handleQtyUpdate(cartItem, delta) {
    if (!cartItem) return;
    const updatedQty = cartItem.quantity + delta;
    if (cartItem.quantity === 1 && delta === -1) {
      // Remove from cart, revert to Add to Cart button
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

  if (loading) return <div className="pcs-center-text">Loading...</div>;
  if (error) return <div className="pcs-center-text pcs-error">{error}</div>;

  return (
    <div className="pcs-bg">
      <div className="pcs-row-container">
        <div className="pcs-category-title">{categoryName}</div>
        <div className="pcs-product-list-grid">
          <div className="pcs-slider">
            {categoryProducts.map(product => {
              const cartItem = cart[product.id];
              return (
                <div key={product.id} className="pcs-card-outer">
                  <div
                    className="pcs-card"
                    tabIndex={0}
                    title={product.name}
                    onClick={() => navigate(`/product/${product.id}`)}
                    style={{ cursor: "pointer" }}
                  >
                    <img src={phoneImg} alt={product.name} className="pcs-card-img" />
                    <div className="pcs-card-name">
                      {product.name.length > 24
                        ? product.name.slice(0, 22) + "..."
                        : product.name}
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
        </div>
      </div>
    </div>
  );
}