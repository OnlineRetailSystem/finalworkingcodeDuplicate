import React, { useEffect, useState } from "react";
import { FiShoppingCart, FiPlus, FiMinus } from "react-icons/fi";
import phoneImg from "../../assets/phone.webp";
import "./ProductDetails.css";
function getUsername() {
  return localStorage.getItem("username");
}
export default function ProductDetails({ productId }) {
  const [product, setProduct] = useState(null);
  const [loading, setLoading] = useState(true);

  // Cart logic
  const [cartItem, setCartItem] = useState(null);
  const [cartLoading, setCartLoading] = useState(false);

  // Product details load
  useEffect(() => {
    async function fetchProduct() {
      try {
        const res = await fetch("http://localhost:8082/products");
        const data = await res.json();
        const prodArr = Array.isArray(data) ? data : [data];
        const found = prodArr.find((p) => String(p.id) === String(productId));
        setProduct(found || null);
      } catch {
        setProduct(null);
      } finally {
        setLoading(false);
      }
    }
    fetchProduct();
  }, [productId]);

  // Cart load for this user/product
  useEffect(() => {
    const username = getUsername();
    if (!productId || !username) return setCartItem(null);
    async function fetchCart() {
      setCartLoading(true);
      try {
        const res = await fetch(`http://localhost:8084/cart/${username}`, { method: "GET" });
        if (!res.ok) { setCartItem(null); return; }
        const cartArr = await res.json();
        const item = Array.isArray(cartArr) ? cartArr.find(i => String(i.productId) === String(productId)) : null;
        setCartItem(item || null);
      } finally {
        setCartLoading(false);
      }
    }
    fetchCart();
  }, [productId]);

  // Add to cart, plus, minus
  async function handleAddToCart() {
    const username = getUsername();
    if (!username) {
      window.location.assign("/usersign");
      return;
    }
    setCartLoading(true);
    try {
      await fetch("http://localhost:8084/cart/add", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, productId, quantity: 1 })
      });
      // Refetch cart item
      const res = await fetch(`http://localhost:8084/cart/${username}`);
      const cartArr = await res.json();
      const item = Array.isArray(cartArr) ? cartArr.find(i => String(i.productId) === String(productId)) : null;
      setCartItem(item || null);
    } finally {
      setCartLoading(false);
    }
  }

  async function handleQtyChange(delta) {
    if (!cartItem) return;
    const newQty = cartItem.quantity + delta;
    if (cartItem.quantity === 1 && delta === -1) {
      // Remove
      setCartLoading(true);
      try {
        await fetch(`http://localhost:8084/cart/${cartItem.id}`, { method: "DELETE" });
        setCartItem(null);
      } finally {
        setCartLoading(false);
      }
      return;
    }
    if (newQty < 1) return;
    setCartLoading(true);
    try {
      await fetch(`http://localhost:8084/cart/${cartItem.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...cartItem, quantity: newQty })
      });
      setCartItem({ ...cartItem, quantity: newQty });
    } finally {
      setCartLoading(false);
    }
  }

  return (
    <main className="pdp-main">
      {loading ? (
        <div className="pdp-loading">Loading...</div>
      ) : !product ? (
        <div className="pdp-error">Product not found.</div>
      ) : (
        <section className="pdp-details-section">
          <div className="pdp-content pdp-content-centered">
            <div className="pdp-img-section">
              <img className="pdp-img-main" src={phoneImg} alt={product.name} />
              <div className="pdp-thumbnail-nav">
                {[...Array(5)].map((_, idx) => (
                  <span key={idx} className={`pdp-dot${idx === 0 ? " active" : ""}`} />
                ))}
              </div>
            </div>
            <div className="pdp-info-section">
              <div className="pdp-breadcrumbs">
                Home &nbsp; &gt; &nbsp; Products &nbsp; &gt; &nbsp;{" "}
                <span>{product.name}</span>
              </div>
              <h2 className="pdp-title">{product.name}</h2>
              <p className="pdp-desc">
                {product.description ||
                  "Lorem ipsum dolor sit amet, consectetur adipisicing elit. Ut amet a a blandit id non viverra massa semper. Varius bibendum suscipit sed mattis turpis enim in ornare. In orci condimentum id in. Sit sodales tempor, sed feugiat sit at fames a tellus."}
              </p>
              <div className="pdp-highlights">
                <span>Highlights:</span>
                <ul>
                  <li>Made with full cotton</li>
                  <li>Slim fit for any body</li>
                  <li>Quality control by JC</li>
                </ul>
              </div>
              <div className="pdp-row pdp-row-big">
                <div>
                  <div className="pdp-label">COLOR</div>
                  <div className="pdp-value pdp-badge">White</div>
                </div>
                <div>
                  <div className="pdp-label">CATEGORY</div>
                  <div className="pdp-value pdp-badge">{product.category}</div>
                </div>
                <div>
                  <div className="pdp-label">STOCK</div>
                  <div className="pdp-value pdp-badge">
                    {product.quantity ?? "—"}
                  </div>
                </div>
                <div className="pdp-price-main">
                  <span className="pdp-currency">$</span>
                  <span>{Number(product.price).toFixed(2)}</span>
                </div>
              </div>
              {/* Add to cart / qty +/- */}
              <div className="pdp-btn-row">
                {cartItem ? (
                  <div className="pdp-qty-row">
                    <button
                      className="pdp-qty-btn"
                      disabled={cartLoading}
                      type="button"
                      onClick={() => handleQtyChange(-1)}
                      aria-label="Decrease quantity"
                    >
                      <FiMinus />
                    </button>
                    <span className="pdp-qty-num">{cartItem.quantity}</span>
                    <button
                      className="pdp-qty-btn"
                      disabled={cartLoading}
                      type="button"
                      onClick={() => handleQtyChange(+1)}
                      aria-label="Increase quantity"
                    >
                      <FiPlus />
                    </button>
                  </div>
                ) : (
                  <button
                    className="pdp-btn pdp-btn-black"
                    type="button"
                    onClick={handleAddToCart}
                    disabled={cartLoading}
                  >
                    <FiShoppingCart style={{ marginRight: "4px" }} />
                    Add to cart
                  </button>
                )}
                <button className="pdp-btn pdp-btn-outline">Add to wishlist</button>
              </div>
            </div>
          </div>
        </section>
      )}
    </main>
  );
}