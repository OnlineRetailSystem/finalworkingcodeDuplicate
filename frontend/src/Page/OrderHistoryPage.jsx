import React from 'react'
import CustomNavbar from '../components/Navbar/Navbar'
import OrderHistory from '../components/OrderHistory/OrderHistory'

export default function OrderHistoryPage() {
  return (
    <>
    <CustomNavbar isSignedIn={true}/>
    <OrderHistory/>
    </>
  )
}
