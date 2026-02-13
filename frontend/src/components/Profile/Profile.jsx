import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import avatarSvg from "../../assets/avatar.png";
import "../Profile/Profile.css";

export default function Profile() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  useEffect(() => {
    const username = localStorage.getItem("username");
    const password = localStorage.getItem("password");
    if (!username || !password) { setLoading(false); return; }
    async function fetchUser() {
      try {
        const basicAuth = 'Basic ' + btoa(`${username}:${password}`);
        const res = await fetch(`http://localhost:8087/user/${username}`, {
          headers: { 'Authorization': basicAuth }
        });
        if (!res.ok) throw new Error("Unauthorized or not found");
        const data = await res.json();
        setUser(data);
      } catch {
        setUser(null);
      } finally {
        setLoading(false);
      }
    }
    fetchUser();
  }, []);

  if (loading) return (
    <div className="profile-bg">
      <div className="profile-loading">Loading...</div>
    </div>
  );
  if (!user) return (
    <div className="profile-bg">
      <div className="profile-error">No profile found or not logged in.</div>
    </div>
  );
  return (
    <div className="profile-bg">
      <div className="profile-container">
        <div className="profile-left-card">
          <img className="profile-avatar" src={avatarSvg} alt="Avatar" />
          <div>
            <div className="profile-fullname">{user.firstName} {user.lastName}</div>
            <div className="profile-username">@{user.username}</div>
            <div className="profile-email">{user.email}</div>
            <div className="profile-mobile">{user.mobileNo}</div>
            <div className="profile-address">{user.address}</div>
          </div>
        </div>
        <div className="profile-info-card">
          <div className="profile-info-row">
            <span className="profile-info-label">First Name</span>
            <span className="profile-info-value">{user.firstName}</span>
          </div>
          <div className="profile-info-row">
            <span className="profile-info-label">Last Name</span>
            <span className="profile-info-value">{user.lastName}</span>
          </div>
          <div className="profile-info-row">
            <span className="profile-info-label">Username</span>
            <span className="profile-info-value">{user.username}</span>
          </div>
          <div className="profile-info-row">
            <span className="profile-info-label">Email</span>
            <span className="profile-info-value">{user.email}</span>
          </div>
          <div className="profile-info-row">
            <span className="profile-info-label">Mobile</span>
            <span className="profile-info-value">{user.mobileNo}</span>
          </div>
          <div className="profile-info-row">
            <span className="profile-info-label">Address</span>
            <span className="profile-info-value">{user.address}</span>
          </div>
          <div style={{ textAlign: "right", marginTop: "32px" }}>
            <button
              className="profile-edit-btn"
              type="button"
              onClick={() => navigate("/profile/edit")}
            >
              Edit
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}