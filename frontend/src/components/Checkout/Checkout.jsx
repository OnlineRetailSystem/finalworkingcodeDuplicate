import React from "react";
import { useNavigate, useLocation } from "react-router-dom";
import Navbar from '../Navbar/Navbar';
 
const CheckoutPage = () => {
  const navigate = useNavigate();
  const location = useLocation();

  // 'amount' is in paise/cents; show in rupees
  const amount = location.state?.amount ?? 0;
  const total = typeof amount === "number" ? (amount / 100) : 0;

  return (
    <>
      <Navbar isSignedIn={true} />
      <div style={{ padding: "2rem", maxWidth: "600px", margin: "0 auto" }}>
        <div
          style={{
            border: "1px solid #ccc",
            borderRadius: "8px",
            padding: "1.5rem",
            boxShadow: "0 2px 8px rgba(0,0,0,0.1)",
          }}
        >
          <h2>Checkout Summary</h2>
          <div style={{ marginBottom: "2rem" }}>
            <h3>Total Amount: ₹{total.toLocaleString(undefined, {minimumFractionDigits: 2})}</h3>
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
            <button
              style={{
                padding: "1rem",
                backgroundColor: "#007bff",
                color: "#fff",
                border: "none",
                borderRadius: "4px",
                cursor: "pointer",
              }}
              onClick={() => navigate("/payments", { state: { amount } })}
            >
              Proceed to Pay
            </button>
            <button
              style={{
                padding: "1rem",
                backgroundColor: "#0b2a44ff",
                color: "#dfd6d6ff",
                border: "none",
                borderRadius: "4px",
                cursor: "pointer",
              }}
              onClick={() => navigate("/profile")}
            >
              Edit shipping details
            </button>
          </div>
        </div>
      </div>
    </>
  );
};
export default CheckoutPage;