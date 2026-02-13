import React, { useState, useRef, useEffect } from "react";
import Swal from "sweetalert2";
import logo from "../../assets/logo.png";
import profileIcon from "../../assets/profile-icon.png"; // Add a profile icon image in assets folder
// import Profile from "..Page/pages/Profile";


const profilePhoto = profileIcon;
export default function Navbar() {
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropdownRef = useRef(null);

  // Close dropdown if clicked outside
  useEffect(() => {
    function handleClickOutside(event) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [dropdownRef]);

  const handleLogoClick = () => {
    window.location.href = "/dashboard";
  };

  const handleLogout = () => {
    setDropdownOpen(false);
    Swal.fire({
      icon: "success",
      title: "Successfully logged out",
      showConfirmButton: false,
      timer: 1500,
      timerProgressBar: true,
      willClose: () => {
        // Clear user credentials from localStorage before redirecting
        localStorage.removeItem('loggedInUser');
        window.location.href = "/home";
      },
    });
  };

  return (
    <nav className="navbar" style={{ height: "60px", padding: "0 20px" }}>
      <div
        className="navbar-left"
        style={{ display: "flex", alignItems: "center", cursor: "pointer" }}
        onClick={handleLogoClick}
      >
        <img
          src={logo}
          alt="Syne-TechMart"
          style={{ height: 30, marginRight: 10, verticalAlign: "middle" }}
        />
      </div>

  <div
    className="navbar-right"
    ref={dropdownRef}
    style={{ position: "relative" }}
  >
    <button
      type="button" // Prevents default submit behavior
      className="navbar-usericon"
      style={{
        background: "transparent",
        borderRadius: "50%",
        width: "40px",
        height: "40px",
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        border: "none",
        cursor: "pointer",
        padding: 0,
      }}
      onClick={() => setDropdownOpen(!dropdownOpen)}
      aria-haspopup="true"
      aria-expanded={dropdownOpen}
      aria-label="User menu"
    >
      <img
        src={profilePhoto}
        alt="Profile"
        style={{ width: "100%", height: "100%", borderRadius: "50%" }}
      />
    </button>

 {dropdownOpen && (
  <div
    className="dropdown-menu"
    style={{
      position: "absolute",
      top: "100%",
      right: 0,
      fontSize: "0.8rem",
      background: "white",
      padding: "0.5rem 0",
      boxShadow: "0 2px 8px rgba(0,0,0,0.1)",
      borderRadius: "4px",
      zIndex: 1000,
      width: "120px", // Optional: controls width for clean line breaks
    }}
  >
    <div
      className="dropdown-item"
      onClick={() => {
        setDropdownOpen(false);
        window.location.href = "/profile";
      }}
      style={{
        padding: "0.5rem 1rem",
        color: "green",
        cursor: "pointer", // Ensures arrow cursor
        whiteSpace: "nowrap",
      }}
    >
      Go to Profile
    </div>
    <div
      className="dropdown-item"
      onClick={handleLogout}
      style={{
        padding: "0.5rem 1rem",
        color: "red",
        cursor: "pointer", // Ensures arrow cursor
        whiteSpace: "nowrap",
      }}
    >
      Logout
    </div>
  </div>
)}

  </div>
</nav>

  );
}