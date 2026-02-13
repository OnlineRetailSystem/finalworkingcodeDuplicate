import React, { useEffect, useState } from "react";

// Images
import smartphoneImg from "../../assets/images/smartphone.png";
import laptopImg from "../../assets/images/laptop.png";
import headphonesImg from "../../assets/images/headphones.png";
import Navbar from "../../components/comp/Navbar";
import OrderSummary from "../../components/comp/OrderSummary";

const API_CART = "http://localhost:8084/cart";

export default function CartPage() {
  // Only use logged-in user, never a default or hardcoded value
  const loggedInUser = JSON.parse(localStorage.getItem("loggedInUser"));
  const [cart, setCart] = useState([]);
  const [loading, setLoading] = useState(true);

  // Redirect or message if not logged in
  if (!loggedInUser?.username) {
    return (
      <div style={{ padding: "50px", fontSize: "1.2em" }}>
        Please log in to see your cart.
      </div>
    );
  }
  const username = loggedInUser.username;

  useEffect(() => {
    fetch(`${API_CART}/${username}`)
      .then((r) => r.json())
      .then((data) => {
        setCart(
          (Array.isArray(data) ? data : []).map((item) => ({
            ...item,
            image:
              item.productName &&
              item.productName.toLowerCase().includes("smartphone")
                ? smartphoneImg
                : item.productName &&
                  item.productName.toLowerCase().includes("laptop")
                ? laptopImg
                : item.productName &&
                  item.productName.toLowerCase().includes("headphone")
                ? headphonesImg
                : "/placeholder.png",
          }))
        );
        setLoading(false);
      });
  }, [username]);

  // Cart math
  const subtotal = cart.reduce(
    (sum, item) => sum + Number(item.price) * Number(item.quantity),
    0
  );
  const shipping = 0;
  const total = subtotal + shipping;

  const handleQtyChange = (id, newQty) => {
    if (newQty <= 0) return handleDelete(id);
    fetch(`${API_CART}/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ quantity: newQty }),
    })
      .then((r) => r.json())
      .then(() => {
        setCart((cart) =>
          cart.map((item) =>
            item.id === id ? { ...item, quantity: newQty } : item
          )
        );
      });
  };

  const handleDelete = (id) => {
    fetch(`${API_CART}/${id}`, { method: "DELETE" }).then(() =>
      setCart((cart) => cart.filter((item) => item.id !== id))
    );
  };

  const handleCheckout = () => {
    // Check if user is logged in before proceeding to checkout
    const loggedInUser = JSON.parse(localStorage.getItem("loggedInUser"));

    if (!loggedInUser?.username) {
      alert("Please log in to proceed to checkout.");
      return;
    }

    // If user is logged in, proceed to checkout
    window.location.href = "/checkout";
  };

  const handleClearCart = () => {
    fetch(`${API_CART}/clear/${username}`, { method: "DELETE" }).then(() =>
      setCart([])
    );
  };

  if (loading) return <div style={{ padding: "48px" }}>Loading...</div>;

  return (
    <div className="cartpage-root">
      <Navbar />
      <div className="cartpage-container">
        <div className="cartpage-main">
          <h1 className="cart-heading">Shopping Cart</h1>
          {cart.length === 0 ? (
            <p>Your cart is empty!</p>
          ) : (
            cart.map((item) => (
              <CartPage
                key={item.id}
                name={item.productName}
                price={item.price}
                image={item.image}
                qty={item.quantity}
                onQtyChange={(newQty) => handleQtyChange(item.id, newQty)}
                onDelete={() => handleDelete(item.id)}
              />
            ))
          )}
          <button
            style={{
              marginTop: "20px",
              padding: "10px 20px",
              fontSize: "1em",
              cursor: "pointer",
            }}
            onClick={() => window.location.href = "/dashboard"}
          >
            Add More Products to Cart
          </button>
          {cart.length > 0 && (
            <button
              style={{
                marginTop: "10px",
                padding: "10px 20px",
                fontSize: "1em",
                cursor: "pointer",
                backgroundColor: "#f44336",
                color: "#fff",
                border: "none",
                borderRadius: "4px",
              }}
              onClick={handleClearCart}
            >
              Clear Cart
            </button>
          )}
        </div>
        <OrderSummary
          subtotal={subtotal}
          shipping={shipping}
          total={total}
          onCheckout={handleCheckout}
        />
      </div>
    </div>
  );
}
