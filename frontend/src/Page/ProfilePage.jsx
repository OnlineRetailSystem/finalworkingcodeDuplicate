import React from 'react'
import CustomNavbar from '../components/Navbar/Navbar'
import Profile from '../components/Profile/Profile'

export default function ProfilePage() {
  return (
    <>
    <CustomNavbar isSignedIn={true}/>
    <Profile/>
    </>
  )
}
