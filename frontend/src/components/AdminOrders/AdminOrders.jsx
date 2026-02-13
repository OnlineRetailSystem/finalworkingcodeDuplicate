import React, { useEffect, useState } from "react";
import { FiTrash2, FiMoreHorizontal, FiChevronDown, FiChevronUp } from "react-icons/fi";
import { useNavigate } from "react-router-dom";
import "./AdminOrders.css";

export default function AdminOrders() {
  const [orders, setOrders] = useState([]);
  const [productsMap, setProductsMap] = useState({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [expandedOrders, setExpandedOrders] = useState(new Set());
  const [refresh, setRefresh] = useState(0);
  const navigate = useNavigate();

  useEffect(() => {
    async function fetchOrders() {
      try {
        setLoading(true);
        const res = await fetch("http://localhost:8083/orders");
        if (!res.ok) throw new Error("Failed to load orders");
        const data = await res.json();
        const ordersArr = Array.isArray(data) ? data : [data];

        // Set default status to "Unknown" if missing
        const normalizedOrders = ordersArr.map(order => ({
          ...order,
          orderStatus: order.orderStatus || "Pending",
        }));
        setOrders(normalizedOrders);

        // Fetch all products for these orders
        const productIds = [
          ...new Set(normalizedOrders.map(order => order.productId))
        ];
        const productFetches = productIds.map(async (pid) => {
          const resp = await fetch(`http://localhost:8082/products/${pid}`);
          if (!resp.ok) throw new Error("Failed fetching product " + pid);
          const productData = await resp.json();
          return { pid, productData };
        });
        const productsArr = await Promise.all(productFetches);
        const productsObj = {};
        for (const { pid, productData } of productsArr) {
          productsObj[pid] = productData;
        }
        setProductsMap(productsObj);
        setError("");
      } catch {
        setError("Failed to load orders or products.");
      } finally {
        setLoading(false);
      }
    }
    fetchOrders();
  }, [refresh]);

  async function handleDelete(orderId) {
    if (!window.confirm("Delete this order?")) return;
    try {
      const res = await fetch(`http://localhost:8083/orders/${orderId}`, {
        method: "DELETE",
      });
      if (!res.ok) throw new Error("Failed to delete");
      setRefresh(r => r + 1);
      setExpandedOrders(prev => {
        const newSet = new Set(prev);
        newSet.delete(orderId);
        return newSet;
      });
    } catch {
      alert("Error deleting order.");
    }
  }

  async function handleStatusChange(orderId, newStatus) {
    try {
      const res = await fetch(`http://localhost:8083/orders/${orderId}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ orderStatus: newStatus }),
      });
      if (!res.ok) throw new Error("Failed to update status");
      setRefresh(r => r + 1);
    } catch {
      alert("Error updating status.");
    }
  }

  function toggleExpand(orderId) {
    setExpandedOrders(prev => {
      const newSet = new Set(prev);
      if (newSet.has(orderId)) newSet.delete(orderId);
      else newSet.add(orderId);
      return newSet;
    });
  }

  if (loading) return <div className="pcs-center-text">Loading orders...</div>;
  if (error) return <div className="pcs-center-text pcs-error">{error}</div>;
  if (orders.length === 0)
    return <div className="pcs-center-text">No orders found.</div>;

  return (
    <div className="pcs-bg">
      <div className="pcs-row-container admin-orders-container">
        <h2 className="admin-orders-title">Orders List</h2>
        <div className="admin-orders-list">
          {orders.map((order) => {
            const product = productsMap[order.productId];
            const isExpanded = expandedOrders.has(order.id);

            // Normalize current orderStatus for display
            const currentStatus =
              order.orderStatus
                ? order.orderStatus // Already a string, keep as is
                : "Unknown"; // fallback if missing

            return (
              <div
                key={order.id}
                className={`admin-order-card ${isExpanded ? "expanded" : ""}`}
              >
                <div
                  className="admin-order-summary"
                  onClick={() => toggleExpand(order.id)}
                  tabIndex={0}
                  role="button"
                  aria-expanded={isExpanded}
                >
                  <div className="admin-order-user">{order.username}</div>
                  <div className="admin-order-product-name">
                    {product ? product.name : "Loading..."}
                  </div>
                  <div className="admin-order-quantity">Qty: {order.quantity}</div>
                  <div className="admin-order-price">
                    ₹{order.totalPrice?.toFixed(2)}
                  </div>
                  <button
                    className="toggle-expand-btn"
                    aria-label={isExpanded ? "Collapse details" : "View more details"}
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleExpand(order.id);
                    }}
                  >
                    {isExpanded ? <FiChevronUp /> : <FiChevronDown />}
                  </button>
                </div>
                {isExpanded && (
                  <div className="admin-order-details">
                    <div>
                      <strong>Category:</strong> {order.category || (product?.category || "N/A")}
                    </div>
                    <div>
                      <strong>Order Date:</strong> {new Date(order.orderDate).toLocaleString()}
                    </div>
                    <div className="admin-order-status-row">
                      <label htmlFor={`status-${order.id}`}>Status:</label>
                      <select
                        id={`status-${order.id}`}
                        value={currentStatus}
                        onChange={(e) => handleStatusChange(order.id, e.target.value)}
                        className={`status-select status-${order.orderStatus?.toLowerCase()}`}
                      >
                        {/* Include "Pending" option as per your request */}
                        <option value="Pending">Pending</option>
                        {/* Existing options */}
                        <option value="Delivered">Delivered</option>
                        <option value="Cancelled">Cancelled</option>
                        {/* You can add "Unknown" if you'd like to display explicitly */}
                        {/* <option value="Unknown">Unknown</option> */}
                      </select>
                    </div>
                    <div className="admin-order-actions">
                      <button
                        type="button"
                        className="pcs-card-btn pcs-delete-btn"
                        onClick={() => handleDelete(order.id)}
                        aria-label="Delete Order"
                        title="Delete Order"
                      >
                        <FiTrash2 size={18} />
                      </button>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}