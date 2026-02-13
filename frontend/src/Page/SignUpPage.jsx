import React from 'react'
import CustomNavbar from '../components/Navbar/Navbar'
import SignUp from '../components/SignUp/SignUp'

export default function SignUpPage() {
  return (
    <>
    <CustomNavbar isSignedIn={false}/>
    <SignUp/>
    </>
  )
}
