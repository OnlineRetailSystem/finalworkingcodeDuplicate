import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { FiEye, FiEyeOff } from "react-icons/fi";
import "../AdminLogin/AdminLogin.css";

export default function AdminLogin() {
  const [username, setUsername] = useState("");
  const [pw, setPw] = useState("");
  const [pwView, setPwView] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();

  function handleLogin(e) {
    e.preventDefault();
    if (username === "admin" && pw === "admin12345") {
      setError("");
      navigate("/admindashboard"); // (Changed)
    } else {
      setError("Incorrect admin username or password.");
    }
  }

  return (
    <div className="admin-login-bg">
      <form className="admin-login-card" onSubmit={handleLogin} autoComplete="off">
        <div className="admin-login-title">Login</div>
        <input
          className="admin-login-input"
          placeholder="Username"
          type="text"
          value={username}
          onChange={e => setUsername(e.target.value)}
          autoFocus
        />
        <div className="admin-login-pw-wrap">
          <input
            className="admin-login-input"
            placeholder="Password"
            type={pwView ? "text" : "password"}
            value={pw}
            onChange={e => setPw(e.target.value)}
          />
          <button
            className="admin-login-eye"
            type="button"
            tabIndex={-1}
            onClick={() => setPwView(v => !v)}
            aria-label={pwView ? "Hide password" : "Show password"}
          >
            {pwView ? <FiEyeOff /> : <FiEye />}
          </button>
        </div>
        <button className="admin-login-btn" type="submit">
          Login
        </button>
        {error && <div className="admin-login-error">{error}</div>}
        <div className="admin-login-support">
          <div>Need help?</div>
          <div>Contact support for assistance.</div>
        </div>
      </form>
    </div>
  );
}