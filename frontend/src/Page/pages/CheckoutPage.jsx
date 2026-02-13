// import React, { useState, useEffect } from 'react';
// import { useNavigate } from 'react-router-dom';
// import Navbar from '../../components/comp/Navbar';
// import OrderSummary from '../../components/comp/OrderSummary';


// const CheckoutPage = () => {
//   const [total, setTotal] = useState(0);
//   const [loading, setLoading] = useState(true);
//   const loggedInUser = JSON.parse(localStorage.getItem('loggedInUser'));
//   const username = loggedInUser?.username;
//   const navigate = useNavigate();
//   // Fetch the latest total from cart, like your cart page does
//   useEffect(() => {
//     fetch(`http://localhost:8084/cart/${username}`)
//       .then(res => res.json())
//       .then(data => {
//         const sum = (data || []).reduce(
//           (acc, item) => acc + item.price * item.quantity,
//           0
//         );
//         setTotal(sum);
//         setLoading(false);
//       })
//       .catch(() => {
//         setTotal(0);
//         setLoading(false);
//       });
//   }, [username]);



// const handleProceedToPay = () => {
//   const amountInCents = Math.round(total * 100); // Convert ₹ to paise
//   navigate('/payments', { state: { amount: amountInCents } });
// };




//   return (
//     <>
//       <Navbar />
//       <div style={{ padding: '20px', maxWidth: '1000px', margin: '0 auto' }}>
//         <h1>Checkout</h1>
//         {/* Your old billing info removed, replaced with button */}
//         <div style={{
//           border: '1px solid #ccc',
//           padding: '15px',
//           borderRadius: '8px',
//           marginBottom: '20px',
//           textAlign: 'center'
//         }}>
//           <button
//             onClick={() => { window.location.href = "/profile"; }}
//             style={{
//               padding: '10px 20px',
//               fontSize: '1em',
//               backgroundColor: '#188afb',
//               color: 'white',
//               border: 'none',
//               borderRadius: '4px',
//               cursor: 'pointer',
//             }}
//           >
//             Edit your existing billing information
//           </button>
//         </div>

//         {/* Show Order Summary */}
//         <div style={{ marginBottom: '20px' }}>
//           {loading ? (
//             <p>Loading order summary...</p>
//           ) : (
//             <OrderSummary
//               subtotal={total}
//               shipping={0}
//               total={total}
//               onCheckout={handleProceedToPay}
//             />
//           )}
//         </div>

//         {/* Proceed to Pay Button */}
//         <button
//           onClick={handleProceedToPay}
//           style={{
//             display: 'block',
//             width: '100%',
//             padding: '12px',
//             backgroundColor: '#278fff',
//             color: 'white',
//             border: 'none',
//             borderRadius: '4px',
//             fontSize: '1.2em',
//             cursor: 'pointer',
//           }}
//         >
//           Proceed to Pay
//         </button>
//       </div>
//     </>
//   );
// };

// export default CheckoutPage;