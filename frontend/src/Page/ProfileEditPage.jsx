import React from 'react'
import ProfileEdit from '../components/ProfileEdit/ProfileEdit'
import CustomNavbar from '../components/Navbar/Navbar'

export default function ProfileEditPage() {
  return (
    <>
    <CustomNavbar isSignedIn={true}/>
    <ProfileEdit/>
    </>
  )
}
