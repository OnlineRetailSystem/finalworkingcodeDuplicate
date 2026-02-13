import React from 'react';
import './Sidebar.css';

export default function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar__brand">
        <span className="brand__dot" /> Syne-Tech Admin
      </div>
      <nav className="sidebar__nav">
        <a href="#">#Dashboard</a>
        <a href="#">#Orders</a>
        <a href="#">#Products</a>
      </nav>
    </aside>
  );
}