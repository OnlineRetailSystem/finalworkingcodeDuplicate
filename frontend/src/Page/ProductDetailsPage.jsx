import React, { useEffect } from "react";
import { useParams } from "react-router-dom";
import ProductDetails from "../components/ProductDetails/ProductDetails";
import ProductCategoryByProductId from "../components/ProductCategoryByProductId/ProductCategoryByProductId";
import CustomNavbar from "../components/Navbar/Navbar";
import Footer from "../components/Footer/Footer";

export default function ProductDetailsPage() {
  const { productId } = useParams();

  useEffect(() => {
    window.scrollTo(0, 0);
  }, [productId]);

  return (
    <>
      <CustomNavbar isSignedIn ={true}/>
      <ProductDetails productId={productId} />
      <ProductCategoryByProductId id={productId} />
      <Footer />
    </>
  );
}
