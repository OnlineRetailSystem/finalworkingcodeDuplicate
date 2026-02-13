import React, { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import avatarSvg from "../../assets/avatar.png";
import "../ProfileEdit/ProfileEdit.css";

export default function ProfileEdit() {
  const [form, setForm] = useState({
    firstName: "",
    lastName: "",
    username: "",
    email: "",
    address: "",
    mobileNo: "",
    // Optionally add serviceProvider if you need
    serviceProvider: ""
  });
  const [loading, setLoading] = useState(true);
  const [saveLoading, setSaveLoading] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();

  useEffect(() => {
    const username = localStorage.getItem("username");
    const password = localStorage.getItem("password");
    if (!username || !password) { setLoading(false); return; }
    async function fetchUser() {
      try {
        const basicAuth = "Basic " + btoa(`${username}:${password}`);
        const res = await fetch(`http://localhost:8087/user/${username}`, {
          headers: { "Authorization": basicAuth }
        });
        if (!res.ok) throw new Error("Unauthorized or not found");
        const data = await res.json();
        setForm({
          firstName: data.firstName || "",
          lastName: data.lastName || "",
          username: data.username || "",
          email: data.email || "",
          address: data.address || "",
          mobileNo: data.mobileNo || "",
          serviceProvider: data.serviceProvider || ""
        });
      } catch {
        setError("Failed to load profile data.");
      } finally {
        setLoading(false);
      }
    }
    fetchUser();
  }, []);

  function handleChange(e) {
    setForm({ ...form, [e.target.name]: e.target.value });
  }

  async function handleSave(e) {
    e.preventDefault();
    setSaveLoading(true);
    setError("");
    const username = localStorage.getItem("username");
    const password = localStorage.getItem("password");
    try {
      const basicAuth = "Basic " + btoa(`${username}:${password}`);
      const res = await fetch(`http://localhost:8087/user/${username}`, {
        method: "PUT",
        headers: { 
          "Authorization": basicAuth,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ ...form, username }),
      });
      if (!res.ok) throw new Error("Update failed.");
      navigate("/profile");
    } catch (err) {
      setError(err.message || "Save failed.");
    }
    setSaveLoading(false);
  }

  if (loading)
    return <div className="profile-edit-bg"><div className="profile-loading">Loading...</div></div>;

  return (
    <div className="profile-edit-bg">
      {/* Cover Banner */}
      <div className="profile-edit-cover" />
      <div className="profile-edit-main">
        <div className="profile-edit-head">
          <div className="profile-ava-wrap">
            <img className="profile-edit-avatar" src={avatarSvg} alt="Avatar" />
            {/* Optionally add an edit icon here */}
          </div>
          <div>
            <div className="profile-edit-fullname">{form.firstName} {form.lastName}</div>
            <div className="profile-edit-meta">@{form.username}</div>
            {/* Optionally add followers/following stat lines */}
          </div>
          <button
            className="profile-edit-savebtn"
            type="submit"
            form="profile-edit-form"
            disabled={saveLoading}
          >
            {saveLoading ? "Saving..." : "Save changes"}
          </button>
        </div>
        <form
          className="profile-edit-form"
          id="profile-edit-form"
          onSubmit={handleSave}
          autoComplete="off"
        >
          <div className="profile-edit-section-title">Personal details</div>
          <div className="profile-edit-grid">
            <div className="profile-edit-field">
              <label>First name</label>
              <input
                name="firstName"
                value={form.firstName}
                onChange={handleChange}
                required
                autoFocus
              />
            </div>
            <div className="profile-edit-field">
              <label>Last name</label>
              <input
                name="lastName"
                value={form.lastName}
                onChange={handleChange}
                required
              />
            </div>
            <div className="profile-edit-field">
              <label>Mobile number</label>
              <input
                name="mobileNo"
                value={form.mobileNo}
                onChange={handleChange}
                required
                type="tel"
                pattern="^[0-9+\-\s()]{6,}$"
              />
            </div>
            <div className="profile-edit-field">
              <label>Email ID</label>
              <input
                name="email"
                value={form.email}
                onChange={handleChange}
                type="email"
                required
              />
            </div>
            <div className="profile-edit-field long">
              <label>Address</label>
              <input
                name="address"
                value={form.address}
                onChange={handleChange}
                type="text"
              />
            </div>
            {/* Optional custom fields, e.g. service provider */}
            {/* <div className="profile-edit-field long">
              <label>Service provider URL</label>
              <input
                name="serviceProvider"
                value={form.serviceProvider}
                onChange={handleChange}
                type="text"
                placeholder="Please enter"
              />
            </div> */}
          </div>
          {error && <div className="profile-edit-error">{error}</div>}
        </form>
      </div>
    </div>
  );
}