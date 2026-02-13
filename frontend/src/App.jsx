import "./App.css";
import Home from "./Page/HomePage";
import SearchResultPage from "./Page/SearchResultPage";
import SignInPage from "./Page/SignInPage";
import SignUpPage from "./Page/SignUpPage";
import ProductDetailsPage from "./Page/ProductDetailsPage";
import DashboardPage from "./Page/DashboardPage";
import { BrowserRouter, Routes, Route, useLocation } from "react-router-dom";
import "./App.css";
import CartPage from "./Page/CartPage";
import OrderHistoryPage from "./Page/OrderHistoryPage";
import AdminLoginPage from "./Page/AdminLoginPage";
import CheckoutPage from "./components/Checkout/Checkout";
import PaymentPage from "./components/Payment/PaymentPage";
import AdminDashboard from "./components/AdminDashboard/AdminDashboard";
import EditProductPage from "./Page/EditProductPage";
import AddProductPage from "./Page/AddProductPage";
import ProfilePage from "./Page/ProfilePage";
import ProfileEditPage from "./Page/ProfileEditPage";
import AdminDashboardPage from "./Page/AdmindashboardPage";
import Charts from "./Page/Charts/Charts";
import AdminOrdersPage from "./Page/AdminOrdersPage";

function useQuery() {
  return new URLSearchParams(useLocation().search);
}
function SearchResultPageWrapper() {
  const query = useQuery();
  const q = query.get("q") || "";
  return <SearchResultPage searchString={q} />;
}
export default function App() {
  return (
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/search" element={<SearchResultPageWrapper />} />
        <Route path="/usersign" element={<SignInPage />} />
        <Route path="/usersignup" element={<SignUpPage />} />
        <Route path="/product/:productId" element={<ProductDetailsPage />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/cart" element={<CartPage/>}/>
        <Route path="/orders" element={<OrderHistoryPage/>}/>
        <Route path="/adminlogin" element={<AdminLoginPage/>}/>
        <Route path="/checkout" element={<CheckoutPage />} />
        <Route path="/payments" element={<PaymentPage />} />
        <Route path="/admindashboard" element={<AdminDashboardPage/>} />
        <Route path="/edit-product/:productId" element={<EditProductPage/>}/>
        <Route path="/add-product" element={<AddProductPage/>}/>
        <Route path="/profile" element={<ProfilePage/>}/>
        <Route path="/profile/edit" element={<ProfileEditPage/>}/>
        <Route path="/chart" element={<Charts/>}/>
        <Route path="/adminorders" element={<AdminOrdersPage/>}/>
      </Routes>
  );
}
