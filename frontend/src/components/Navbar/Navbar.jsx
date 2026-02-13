import React, { useState, useEffect } from "react";
import {
  Navbar,
  Container,
  Nav,
  Dropdown,
  Form,
  FormControl,
  Offcanvas,
} from "react-bootstrap";
import {
  FiSearch,
  FiShoppingCart,
  FiUser,
  FiLogIn,
  FiUserPlus,
  FiMenu,
  FiX,
  FiHome,
  FiLogOut
} from "react-icons/fi";
import { MdPersonOutline } from "react-icons/md";
import { RiListUnordered } from "react-icons/ri";
import { AiOutlineHeart } from "react-icons/ai";
import { useNavigate, Link } from "react-router-dom";
import Swal from 'sweetalert2';
import "./Navbar.css";

const CustomNavbar = ({ isSignedIn = false, cartCount }) => {
  const [showSearch, setShowSearch] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [showOffcanvas, setShowOffcanvas] = useState(false);
  const [isTabletOrPhone, setIsTabletOrPhone] = useState(window.innerWidth <= 991);
  const navigate = useNavigate();
  useEffect(() => {
    const handleResize = () => {
      setIsTabletOrPhone(window.innerWidth <= 991);
    };
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);
  const handleCartClick = () => {
    setShowOffcanvas(false);
    if (isSignedIn) {
      navigate("/cart");
    } else {
      navigate("/usersign");
    }
  };
  const handleHomeLogoClick = () => {
    setShowOffcanvas(false);
    const username = localStorage.getItem("username");
    if (username) {
      navigate("/dashboard");
    } else {
      navigate("/");
    }
  };
  const handleSearchToggle = () => {
    setShowSearch((prev) => !prev);
    if (showSearch) setSearchQuery("");
  };
  const handleSearchSubmit = (e) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      setShowSearch(false);
      setShowOffcanvas(false);
      navigate(`/search?q=${encodeURIComponent(searchQuery.trim())}`);
    }
  };
  const handleOffcanvasClose = () => setShowOffcanvas(false);
  const handleOffcanvasShow = () => setShowOffcanvas(true);

  // SweetAlert confirmation
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
        setShowOffcanvas(false);
        navigate("/");
      }
    });
  }

  const renderSearchBar = () => (
    <Form onSubmit={handleSearchSubmit} className="search-form">
      <FormControl
        type="search"
        placeholder="Search products..."
        className="search-input"
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        autoFocus={!isTabletOrPhone && showSearch}
      />
      <button type="submit" className="search-submit" aria-label="Submit search">
        <FiSearch className="search-icon" />
      </button>
      {isTabletOrPhone && (
        <button
          type="button"
          className="search-close"
          onClick={() => setSearchQuery("")}
          aria-label="Clear search"
        >
          <FiX />
        </button>
      )}
    </Form>
  );
  return (
    <>
      <Navbar expand={false} className="custom-navbar">
        <Container fluid className={isTabletOrPhone ? "navbar-container-mobile" : "navbar-container"}>
          {isTabletOrPhone ? (
            <div className="navbar-mobile-row">
              <Navbar.Toggle
                aria-controls="navbar-offcanvas"
                className="navbar-toggler"
                onClick={handleOffcanvasShow}
              >
                <FiMenu className="nav-icon" />
              </Navbar.Toggle>
              <div className="navbar-search-mobile">
                {renderSearchBar()}
              </div>
              <Navbar.Brand
                as="div"
                className="company-name mobile-brand"
                style={{ cursor: "pointer" }}
                onClick={handleHomeLogoClick}
                tabIndex={0}
                role="button"
              >
                SparkCart
              </Navbar.Brand>
            </div>
          ) : (
            <>
              <Navbar.Brand
                as="div"
                className="company-name"
                style={{ cursor: "pointer" }}
                tabIndex={0}
                role="button"
                onClick={handleHomeLogoClick}
              >
                SparkCart
              </Navbar.Brand>
              <div className={`navbar-search-center${showSearch ? " show" : ""}`}>
                {showSearch && renderSearchBar()}
              </div>
              <Nav className="ms-auto navbar-icons">
                <div className="navbar-icon-group">
                  <Nav.Link
                    className={`icon-wrapper search-icon-wrapper ${
                      showSearch ? "active" : ""
                    }`}
                    onClick={handleSearchToggle}
                    aria-label="Search"
                  >
                    <FiSearch className="nav-icon" />
                    <span className="icon-tooltip">Search</span>
                  </Nav.Link>
                  <Nav.Link
                    className="icon-wrapper cart-icon-wrapper"
                    onClick={handleCartClick}
                    aria-label="Cart"
                  >
                    <FiShoppingCart className="nav-icon" />
                    <span className="icon-tooltip">Cart</span>
                  </Nav.Link>
                  <Dropdown align="end" className="account-dropdown">
                    <Dropdown.Toggle
                      as={Nav.Link}
                      className="icon-wrapper account-icon-wrapper"
                      id="account-dropdown"
                      aria-label="Account"
                    >
                      <FiUser className="nav-icon" />
                      <span className="icon-tooltip account-tooltip">Account</span>
                    </Dropdown.Toggle>
                    <Dropdown.Menu
                      className="custom-dropdown-menu dropdown-menu-end"
                    >
                      {isSignedIn ? (
                        <>
                          <Dropdown.Item
                            as={Link}
                            to="/profile"
                            className="dropdown-item-custom profile-option"
                          >
                            <MdPersonOutline className="dropdown-icon profile-color" />
                            <span className="dropdown-label">Profile</span>
                          </Dropdown.Item>
                          <Dropdown.Item
                            as={Link}
                            to="/orders"
                            className="dropdown-item-custom orders-option"
                          >
                            <RiListUnordered className="dropdown-icon orders-color" />
                            <span className="dropdown-label">Orders</span>
                          </Dropdown.Item>
                          <Dropdown.Item
                            as={Link}
                            to="/wishlist"
                            className="dropdown-item-custom wishlist-option"
                          >
                            <AiOutlineHeart className="dropdown-icon wishlist-color" />
                            <span className="dropdown-label">Wishlist</span>
                          </Dropdown.Item>
                          <Dropdown.Divider />
                          <Dropdown.Item
                            as="button"
                            className="dropdown-item-custom logout-item"
                            onClick={handleLogout}
                          >
                            <FiLogOut className="dropdown-icon logout-color" />
                            <span className="dropdown-label">Logout</span>
                          </Dropdown.Item>
                        </>
                      ) : (
                        <>
                          <Dropdown.Item
                            as={Link}
                            to="/usersign"
                            className="dropdown-item-custom signin-item"
                          >
                            <FiLogIn className="dropdown-icon" /> Sign In
                          </Dropdown.Item>
                          <Dropdown.Item
                            as={Link}
                            to="/usersignup"
                            className="dropdown-item-custom signup-item"
                          >
                            <FiUserPlus className="dropdown-icon" /> Sign Up
                          </Dropdown.Item>
                          <Dropdown.Item
                            as={Link}
                            to="/adminlogin"
                            className="dropdown-item-custom adminlogin-item"
                          >
                            <FiUser className="dropdown-icon" /> Admin Login
                          </Dropdown.Item>
                        </>
                      )}
                    </Dropdown.Menu>
                  </Dropdown>
                </div>
              </Nav>
            </>
          )}
        </Container>
      </Navbar>
      <Offcanvas
        show={showOffcanvas}
        onHide={handleOffcanvasClose}
        placement="start"
        className="mobile-menu"
        id="navbar-offcanvas"
      >
        <Offcanvas.Header closeButton>
          <Offcanvas.Title>Menu</Offcanvas.Title>
        </Offcanvas.Header>
        <Offcanvas.Body>
          <Nav className="flex-column">
            <Nav.Link
              as={Link}
              to="/"
              className="mobile-nav-item"
              onClick={handleOffcanvasClose}
            >
              <FiHome className="mobile-nav-icon" /> Home
            </Nav.Link>
            <Nav.Link
              className="mobile-nav-item"
              onClick={() => {
                handleOffcanvasClose();
                handleCartClick();
              }}
            >
              <FiShoppingCart className="mobile-nav-icon" /> Cart
            </Nav.Link>
            {isSignedIn ? (
              <>
                <Nav.Link
                  as={Link}
                  to="/profile"
                  className="mobile-nav-item"
                  onClick={handleOffcanvasClose}
                >
                  <MdPersonOutline className="mobile-nav-icon profile-color" /> Profile
                </Nav.Link>
                <Nav.Link
                  as={Link}
                  to="/orders"
                  className="mobile-nav-item"
                  onClick={handleOffcanvasClose}
                >
                  <RiListUnordered className="mobile-nav-icon orders-color" /> Orders
                </Nav.Link>
                <Nav.Link
                  as={Link}
                  to="/wishlist"
                  className="mobile-nav-item"
                  onClick={handleOffcanvasClose}
                >
                  <AiOutlineHeart className="mobile-nav-icon wishlist-color" /> Wishlist
                </Nav.Link>
                <Nav.Link
                  as="button"
                  className="mobile-nav-item logout-item"
                  onClick={() => {
                    handleOffcanvasClose();
                    handleLogout();
                  }}
                >
                  <FiLogOut className="mobile-nav-icon logout-color" /> Logout
                </Nav.Link>
              </>
            ) : (
              <>
                <Nav.Link
                  as={Link}
                  to="/usersign"
                  className="mobile-nav-item signin-item"
                  onClick={handleOffcanvasClose}
                >
                  <FiLogIn className="mobile-nav-icon" /> Sign In
                </Nav.Link>
                <Nav.Link
                  as={Link}
                  to="/usersignup"
                  className="mobile-nav-item signup-item"
                  onClick={handleOffcanvasClose}
                >
                  <FiUserPlus className="mobile-nav-icon" /> Sign Up
                </Nav.Link>
                <Nav.Link
                  as={Link}
                  to="/adminlogin"
                  className="mobile-nav-item adminlogin-item"
                  onClick={handleOffcanvasClose}
                >
                  <FiUser className="mobile-nav-icon" /> Admin Login
                </Nav.Link>
              </>
            )}
          </Nav>
        </Offcanvas.Body>
      </Offcanvas>
    </>
  );
};

export default CustomNavbar;