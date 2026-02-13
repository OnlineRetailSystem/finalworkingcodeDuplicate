import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import "../SignIn/SignIn.css";

export default function SignIn() {
  const [form, setForm] = useState({
    username: "",
    password: "",
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();

  function handleChange(e) {
    setForm({ ...form, [e.target.name]: e.target.value });
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setLoading(true);
    setError("");
    try {
      const res = await fetch("http://localhost:8087/signin", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      if (!res.ok) throw new Error("Invalid credentials");
      localStorage.setItem("username", form.username);
      localStorage.setItem("password", form.password); // for dev only
      navigate("/dashboard");
    } catch (err) {
      setError("Sign In failed");
    }
    setLoading(false);
  }

  return (
    <div style={{ position: "relative", minHeight: "100vh", width: "100vw" }}>
      {/* Animated gradient background */}
      <div className="bg"></div>
      <div className="bg bg2"></div>
      <div className="bg bg3"></div>
      {/* Centered form */}
      <div className="auth-bg">
        <form className="auth-form" onSubmit={handleSubmit}>
          <h2 className="auth-title">Sign In</h2>
          <div className="auth-fieldgroup">
            <input
              name="username"
              className="auth-input"
              placeholder="Username"
              value={form.username}
              onChange={handleChange}
              autoFocus
              required
            />
          </div>
          <div className="auth-fieldgroup">
            <input
              name="password"
              className="auth-input"
              placeholder="Password"
              type="password"
              value={form.password}
              onChange={handleChange}
              required
            />
          </div>
          {error && <div className="auth-error">{error}</div>}
          <button className="auth-btn" type="submit" disabled={loading}>
            {loading ? "Signing In..." : "Sign In"}
          </button>
          <div className="auth-alt">
            Don't have an account?
            <button
              type="button"
              className="auth-link"
              onClick={() => navigate("/usersignup")}
            >
              Sign Up
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}