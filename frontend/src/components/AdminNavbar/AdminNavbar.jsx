import React, { useEffect, useState } from "react";
import { Navbar, Container, Nav } from "react-bootstrap";
import { useNavigate } from "react-router-dom";
import { FiLogOut, FiBarChart2 } from "react-icons/fi";
import { FaPlus , FaBoxOpen} from "react-icons/fa";
import Swal from "sweetalert2";
import "../AdminNavbar/AdminNavbar.css";

export default function AdminNavbar() {
  const [isTabletOrPhone, setIsTabletOrPhone] = useState(window.innerWidth <= 991);
  const navigate = useNavigate();

  useEffect(() => {
    const handleResize = () => setIsTabletOrPhone(window.innerWidth <= 991);
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  function handleLogoClick() {
    navigate("/admindashboard");
  }

  function handleAddProduct() {
    navigate("/add-product");
  }

  function handleChart() {
    navigate("/chart"); // Or wherever your analytics/summary lives
  }


  function handleOrders() {
    navigate("/adminorders");
  }


  function handleLogout() {
    Swal.fire({
      title: "Are you sure you want to logout?",
      icon: "warning",
      showCancelButton: true,
      confirmButtonText: "Logout",
      cancelButtonText: "Cancel",
      confirmButtonColor: "#1976d2"
    }).then((result) => {
      if (result.isConfirmed) {
        localStorage.removeItem("username");
        localStorage.removeItem("userid");
        navigate("/");
      }
    });
  }

  return (
    <Navbar expand={false} className="custom-navbar">
      <Container
        fluid
        className={isTabletOrPhone ? "navbar-container-mobile" : "navbar-container"}
      >
        <Navbar.Brand
          as="div"
          className="company-name"
          style={{ cursor: "pointer" }}
          tabIndex={0}
          role="button"
          onClick={handleLogoClick}
        >
          SparkCart-Admin
        </Navbar.Brand>
        <Nav className="ms-auto navbar-icons">
          <div className="admin-navbar-actions">
            <button
              className="admin-navbar-btn admin-navbar-add"
              type="button"
              onClick={handleAddProduct}
            >
              <FaPlus style={{ marginRight: 7 }} /> Add Product
            </button>
            <button
              className="admin-navbar-btn admin-navbar-chart"
              type="button"
              onClick={handleChart}
            >
              <FiBarChart2 style={{ marginRight: 7 }} /> Chart
            </button>


            <button
              className="admin-navbar-btn"
              type="button"
              onClick={handleOrders}
            >
              <FaBoxOpen style={{ marginRight: 7 }} /> Orders
            </button>


            <button
              className="admin-navbar-btn admin-navbar-logout"
              type="button"
              onClick={handleLogout}
            >
              <FiLogOut style={{ marginRight: 7 }} /> Logout
            </button>
          </div>
        </Nav>
      </Container>
    </Navbar>
  );
}