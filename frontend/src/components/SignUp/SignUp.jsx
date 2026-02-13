import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import "../SignUp/SignUp.css";

export default function SignUp() {
  const [form, setForm] = useState({
    username: "",
    email: "",
    password: "",
    firstName: "",
    lastName: ""
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const navigate = useNavigate();

  function handleChange(e) {
    setForm({ ...form, [e.target.name]: e.target.value });
  }
  async function handleSubmit(e) {
    e.preventDefault();
    setLoading(true); setError(""); setSuccess("");
    try {
      const res = await fetch("http://localhost:8087/signup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form)
      });
      if (!res.ok) throw new Error("Registration failed");
      setSuccess("User registered successfully!");
      setTimeout(() => navigate("/dashboard"), 1200);
    } catch (err) {
      setError("Signup failed");
    }
    setLoading(false);
  }

  return (
    <div className="auth-bg">
      <form className="auth-form" onSubmit={handleSubmit} autoComplete="off">
        <h2 className="auth-title">Sign Up</h2>
        <div className="auth-fieldgroup">
          <input name="firstName" className="auth-input" placeholder="First Name"
            value={form.firstName} onChange={handleChange} autoFocus required />
        </div>
        <div className="auth-fieldgroup">
          <input name="lastName" className="auth-input" placeholder="Last Name"
            value={form.lastName} onChange={handleChange} required />
        </div>
        <div className="auth-fieldgroup">
          <input name="username" className="auth-input" placeholder="Username"
            value={form.username} onChange={handleChange} required />
        </div>
        <div className="auth-fieldgroup">
          <input name="email" className="auth-input" placeholder="Email"
            value={form.email} onChange={handleChange} type="email" required />
        </div>
        <div className="auth-fieldgroup">
          <input name="password" className="auth-input" placeholder="Password"
            value={form.password} onChange={handleChange} type="password" required />
        </div>
        {error && <div className="auth-error">{error}</div>}
        {success && <div className="auth-success">{success}</div>}
        <button className="auth-btn" type="submit" disabled={loading}>
          {loading ? "Signing Up..." : "Sign Up"}
        </button>
        <div className="auth-alt">
          Already have an account?
          <button type="button" className="auth-link" onClick={() => navigate("/usersign")}>
            Sign In
          </button>
        </div>
      </form>
    </div>
  );
}
