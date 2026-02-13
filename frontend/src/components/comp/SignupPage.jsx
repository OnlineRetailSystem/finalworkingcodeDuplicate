import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import Swal from "sweetalert2";
import "./SignupPage.css";
const API_URL = "http://localhost:8081";
const SAMPLE_TERMS = [
  "Your account is personal and must not be shared.",
  "All purchases are subject to our return policy.",
  "You consent to receiving notifications about your orders.",
  "We reserve the right to suspend accounts for fraudulent activity.",
  "You agree to our privacy policy and data usage terms."
];
const SignupPage = () => {
  const [formType, setFormType] = useState("signup");
  const [showTerms, setShowTerms] = useState(false);
  const [signupData, setSignupData] = useState({
    firstName: "",
    lastName: "",
    username: "",
    email: "",
    password: "",
    agreeTnC: false,
  });
  const [loginData, setLoginData] = useState({ username: "", password: "" });
  const navigate = useNavigate();
  const handleChange = (e, type) => {
    if (type === "signup") {
      if (e.target.name === "agreeTnC") {
        setSignupData({ ...signupData, agreeTnC: e.target.checked });
      } else {
        setSignupData({ ...signupData, [e.target.name]: e.target.value });
      }
    } else {
      setLoginData({ ...loginData, [e.target.name]: e.target.value });
    }
  };
  const handleSignup = async (e) => {
    e.preventDefault();
    if (!signupData.agreeTnC) {
      Swal.fire({
        icon: 'error',
        title: 'Error',
        text: 'You must agree to the Terms & Conditions.',
      });
      return;
    }
    if (!signupData.firstName || !signupData.lastName || !signupData.username || !signupData.email || !signupData.password) {
      Swal.fire({
        icon: 'error',
        title: 'Error',
        text: 'All fields are required.',
      });
      return;
    }
    try {
      const res = await fetch(`${API_URL}/signup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          username: signupData.username,
          email: signupData.email,
          password: signupData.password,
          firstName: signupData.firstName,
          lastName: signupData.lastName,
        }),
      });
      const msg = await res.text();
      if (res.ok) {
        Swal.fire({
          icon: 'success',
          title: 'Success',
          text: msg,
        }).then(() => {
          setTimeout(() => setFormType("login"), 1200);
        });
      } else {
        Swal.fire({
          icon: 'error',
          title: 'Signup Failed',
          text: msg || "Signup failed.",
        }).then(() => {
          // Clear signup form on invalid credentials
          setSignupData({ firstName: "", lastName: "", username: "", email: "", password: "", agreeTnC: false });
        });
      }
    } catch (err) {
      Swal.fire({
        icon: 'error',
        title: 'Error',
        text: 'Cannot connect to server.',
      });
    }
  };
const handleLogin = async (e) => {
  e.preventDefault();
  try {
    const res = await fetch(`${API_URL}/signin`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: loginData.username,
        password: loginData.password,
      }),
    });
    const msg = await res.text();

    if (res.ok) {
      // Fetch full user info by username
      fetch(`/user/username/${loginData.username}`)
        .then(res => {
          if (!res.ok) throw new Error('User not found');
          return res.json();
        })
        .then(user => {
          localStorage.setItem('loggedInUser', JSON.stringify({ username: user.username, id: user.id }));
          Swal.fire({ icon: 'success', title: 'Success', text: msg }).then(() => {
            navigate('/dashboard');
          });
        })
        .catch(() => {
          // fallback if fetch fails
          localStorage.setItem('loggedInUser', JSON.stringify({ username: loginData.username }));
          Swal.fire({ icon: 'success', title: 'Success', text: msg }).then(() => {
            navigate('/dashboard');
          });
        });
    } else {
      Swal.fire({
        icon: 'error',
        title: 'Login Failed',
        text: msg || "Login failed.",
      }).then(() => {
        setLoginData({ username: "", password: "" });
      });
    }
  } catch (err) {
    Swal.fire({
      icon: 'error',
      title: 'Error',
      text: 'Cannot connect to server.',
    });
  }
};
  const handleBack = () => { navigate("/home"); };
  return (
    <>
      <div className="electrobolt-signup fadein">
        <div className="left-side">
          <div className="heading-row">
            <div>
              <span className="site-title">Syne-</span>
              <br />
              <span className="site-title">TechMart</span>
            </div>
            <button onClick={handleBack} className="back-btn">Back to Store</button>
          </div>
          <div className="subtitle">Bringing the Best Deals,<br />Right to Your Door</div>
        </div>
        <div className="right-side">
          <h2>{formType === "signup" ? "Create an account" : "Sign in"}</h2>
          <p className="toggle-link">
            {formType === "signup" ? (
              <>Already have an account? <span onClick={() => { setFormType("login"); setSignupData({ firstName: "", lastName: "", username: "", email: "", password: "", agreeTnC: false });}} style={{color:"#188afb", cursor:"pointer"}}>Log in</span></>
            ) : (
              <>New? <span onClick={() => { setFormType("signup"); setLoginData({ username: "", password: "" });}} style={{color:"#188afb", cursor:"pointer"}}>Sign up</span></>
            )}
          </p>
          <form onSubmit={formType === "signup" ? handleSignup : handleLogin}>
            {formType === "signup" ? (
              <>
                <div style={{display:"flex", gap:"10px"}}>
                  <input type="text" name="firstName" placeholder="First name" value={signupData.firstName}
                    onChange={e => handleChange(e, "signup")}
                    required style={{flex:1}} />
                  <input type="text" name="lastName" placeholder="Last name" value={signupData.lastName}
                    onChange={e => handleChange(e, "signup")}
                    required style={{flex:1}} />
                </div>
                <input type="text" name="username" placeholder="Username"
                  value={signupData.username} onChange={e => handleChange(e, "signup")} required />
                <input type="email" name="email" placeholder="Email"
                  value={signupData.email} onChange={e => handleChange(e, "signup")} required />
                <input type="password" name="password" placeholder="Enter your password"
                  value={signupData.password} onChange={e => handleChange(e, "signup")} required />
                <div className="tnc-row">
                  <input
                    type="checkbox"
                    name="agreeTnC"
                    checked={signupData.agreeTnC}
                    onChange={e => handleChange(e, "signup")}
                    required
                  />
                  <span>
                    I agree to the <button type="button" className="tnc-link"
                      style={{background:'none',color:'#188afb',border:'none',textDecoration:'underline',cursor:'pointer'}}
                      onClick={() => setShowTerms(true)}>
                      Terms & Conditions
                    </button>
                  </span>
                </div>
              </>
            ) : (
              <>
                <input
                  type="text"
                  name="username"
                  placeholder="Username"
                  value={loginData.username}
                  onChange={e => handleChange(e, "login")}
                  required
                />
                <input
                  type="password"
                  name="password"
                  placeholder="Enter your password"
                  value={loginData.password}
                  onChange={e => handleChange(e, "login")}
                  required
                />
              </>
            )}
            <button type="submit" className="main-action">
              {formType === "signup" ? "Create account" : "Sign in"}
            </button>
          </form>
        </div>
      </div>
      {showTerms && (
        <div className="modal-bg" onClick={() => setShowTerms(false)}>
          <div className="modal-content" onClick={e => e.stopPropagation()}>
            <h3>Terms & Conditions</h3>
            <ul>
              {SAMPLE_TERMS.map((t,idx)=><li key={idx}>{t}</li>)}
            </ul>
            <button onClick={() => setShowTerms(false)} className="close-btn">Close</button>
          </div>
        </div>
      )}
    </>
  );
};
export default SignupPage;