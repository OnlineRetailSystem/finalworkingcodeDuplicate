import React from 'react';
import CustomNavbar from '../components/Navbar/Navbar';
import SearchResult from '../components/SearchResult/SearchResult';
import ProductCategorySliders from '../components/ProductCategorySlider/ProductCategorySlider';
import Footer from '../components/Footer/Footer';

export default function SearchResultPage({ searchString }) {
  return (
    <>
      <CustomNavbar isSignedIn={true}/>
      <SearchResult searchString={searchString} />
      <ProductCategorySliders />
      <Footer />
    </>
  );
}