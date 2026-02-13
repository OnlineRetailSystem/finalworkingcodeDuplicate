import React from 'react';
import './Charts.css';

// Use your existing Navbar
import CustomNavbar from '../../components/Navbar/Navbar';

// Our components
import Sidebar from '../../components/Sidebar/Sidebar';
import StatsCards from '../../components/StatsCards/StatsCards';
import OrdersPieChart from '../../components/Charts/OrdersPieChart/OrdersPieChart';
import ProductsByCategoryBar from '../../components/Charts/ProductsByCategoryBar/ProductsByCategoryBar';
import AdminNavbar from '../../components/AdminNavbar/AdminNavbar';

export default function Charts() {
  return (
    <>
      {/* Top Navbar (You already have it) */}
      {/* <CustomNavbar isSignedIn={true} /> */}
      <AdminNavbar/>

      {/* Page layout */}
      <div className="admin">
        {/* <Sidebar /> */}

        <main className="admin__main">
          {/* Summary cards */}
          <StatsCards />

          {/* Charts grid */}
          <div className="charts__grid">
            <div className="charts__item"><OrdersPieChart /></div>
            <div className="charts__item"><ProductsByCategoryBar /></div>
          </div>

          {/* Placeholder for future widgets (Recent Orders, Low Stock etc.) */}
          {/* <div className="panel"><div className="panel__header"><h3>Recent Orders</h3></div></div> */}
        </main>
      </div>
    </>
  );
}
