import React from 'react'
import CustomNavbar from '../components/Navbar/Navbar'
import Banner from '../components/Banner/Banner'
import ProductCategorySliders from '../components/ProductCategorySlider/ProductCategorySlider'
import Footer from '../components/Footer/Footer'

export default function Dashboard() {
  return (
    <>
    <CustomNavbar isSignedIn={true}/>
    <Banner/>
    <ProductCategorySliders/>
    <Footer/>
    </>
  )
}
