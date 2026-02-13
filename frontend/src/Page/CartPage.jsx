import React from 'react'
import Cart from '../components/Cart/Cart'
import CustomNavbar from '../components/Navbar/Navbar'

export default function CartPage() {
  return (
    <>
    <CustomNavbar isSignedIn={true}/>
    <Cart/>
    </>
  )
}
