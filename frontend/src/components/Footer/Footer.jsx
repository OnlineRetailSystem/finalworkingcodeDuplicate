import React from "react";
import {
  FaFacebookF,
  FaTwitter,
  FaInstagram,
  FaYoutube,
  FaStore,
  FaAd,
  FaGift,
  FaRegQuestionCircle,
  FaCcVisa,
  FaCcMastercard,
  FaCcAmex
} from "react-icons/fa";
/* Removed import of SiRupay as it causes runtime error */
import { MdCreditCard, MdHelp } from "react-icons/md";

import "./Footer.css";

const Footer = () => {
  return (
    <footer className="footer">
      <div className="footer-top">
        <div className="footer-col">
          <div className="footer-title">ABOUT</div>
          <ul>
            <li><a href="#">Contact Us</a></li>
            <li><a href="#">About Us</a></li>
            <li><a href="#">Careers</a></li>
            <li><a href="#">SparkCart Stories</a></li>
            <li><a href="#">Press</a></li>
            <li><a href="#">Corporate Information</a></li>
          </ul>
        </div>
        <div className="footer-col">
          <div className="footer-title">HELP</div>
          <ul>
            <li><a href="#">Payments</a></li>
            <li><a href="#">Shipping</a></li>
            <li><a href="#">Cancellation & Returns</a></li>
            <li><a href="#">FAQ</a></li>
          </ul>
        </div>
        <div className="footer-col policy-col">
          <div className="footer-title">CONSUMER POLICY</div>
          <ul>
            <li><a href="#">Cancellation & Returns</a></li>
            <li><a href="#">Terms Of Use</a></li>
            <li><a href="#">Security</a></li>
            <li><a href="#">Privacy</a></li>
            <li><a href="#">Sitemap</a></li>
            <li><a href="#">Grievance Redressal</a></li>
            <li><a href="#">EPR Compliance</a></li>
          </ul>
        </div>
        <div className="footer-col contact-col">
          <div className="footer-title">Mail Us:</div>
          <div className="footer-contact-text">
            SparkCart, <br />
            Pune, Maharashtra, India<br />
          </div>
          <div className="footer-social-title">Social:</div>
          <div className="footer-social">
            <a href="#"><FaFacebookF /></a>
            <a href="#"><FaTwitter /></a>
            <a href="#"><FaYoutube /></a>
            <a href="#"><FaInstagram /></a>
          </div>
        </div>
        <div className="footer-col registered-col">
          <div className="footer-title">Registered Office Address:</div>
          <div className="footer-contact-text">
            SparkCart, Pune, Maharashtra, India<br />
            CIN : U51109KA2012PTC066107<br />
            <span className="footer-reg-contact">
              Telephone: <a href="tel:044-45614700" className="footer-blue">044-45614700</a> / <a href="tel:044-67415800" className="footer-blue">044-67415800</a>
            </span>
          </div>
        </div>
      </div>
      <div className="footer-bottom">
        <div className="footer-bottom-row">
          <a href="#" className="footer-bottom-link"><FaStore />&ensp;Become a Seller</a>
          <a href="#" className="footer-bottom-link"><FaAd />&ensp;Advertise</a>
          <a href="#" className="footer-bottom-link"><FaGift />&ensp;Gift Cards</a>
          <a href="#" className="footer-bottom-link"><MdHelp />&ensp;Help Center</a>
          <span className="footer-copyright">© 2007-2025 SparkCart.com</span>
          <div className="footer-payments">
            <FaCcVisa title="Visa" />
            <FaCcMastercard title="MasterCard" />
            <FaCcAmex title="Amex" />
            {/* <SiRupay title="Rupay" /> */}
            <MdCreditCard title="Credit Card" />
            {/* Add more icons as needed */}
          </div>
        </div>
      </div>
    </footer>
  );
};

export default Footer;
