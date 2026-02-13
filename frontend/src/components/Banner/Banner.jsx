import React, { useState, useEffect, useRef } from "react";
import banner1 from "../../assets/banner/1.png";
import banner2 from "../../assets/banner/2.png";
import banner3 from "../../assets/banner/3.png";
import banner4 from "../../assets/banner/4.png";
import banner5 from "../../assets/banner/5.png";
import banner6 from "../../assets/banner/6.png";
import "../Banner/Banner.css";

const banners = [
  { id: 1, image: banner1 },
  { id: 2, image: banner2 },
  { id: 3, image: banner3 },
  { id: 4, image: banner4 },
  { id: 5, image: banner5 },
  { id: 6, image: banner6 },
];

const Banner = () => {
  const [index, setIndex] = useState(0);
  const timer = useRef();
  useEffect(() => {
    timer.current = setInterval(() => {
      setIndex((prev) => (prev + 1) % banners.length);
    }, 4000);
    return () => clearInterval(timer.current);
  }, []);
  const handleDot = (idx) => {
    setIndex(idx);
    resetInterval();
  };
  const resetInterval = () => {
    clearInterval(timer.current);
    timer.current = setInterval(() => {
      setIndex((prev) => (prev + 1) % banners.length);
    }, 4000);
  };
  return (
    <div className="main-banner-outer">
      <div className="main-banner-center-bg">
        <div className="main-banner-img-wrapper">
          <img
            key={banners[index].id}
            src={banners[index].image}
            alt=""
            className="main-banner-img"
            draggable={false}
          />
        </div>
        <div className="main-banner-dots">
          {banners.map((_, idx) => (
            <button
              key={idx}
              className={`main-banner-dot ${idx === index ? "active" : ""}`}
              onClick={() => handleDot(idx)}
              aria-label={`Go to slide ${idx + 1}`}
              tabIndex={0}
            ></button>
          ))}
        </div>
      </div>
    </div>
  );
};
export default Banner;