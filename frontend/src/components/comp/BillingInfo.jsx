import React, { useState, useEffect } from 'react';

const BillingInfo = () => {
  const [userInfo, setUserInfo] = useState(null);
  const [isEditing, setIsEditing] = useState(false);
  const loggedInUser = JSON.parse(localStorage.getItem("loggedInUser"));
  const userId = loggedInUser?.id;
  const username = loggedInUser?.username;

useEffect(() => {
  fetch(`http://localhost:8081/user/username/${username}`)
    .then(res => res.json())
    .then(data => {
      setUserInfo(data);
    })
    .catch(err => {
      console.error('Failed to fetch user info:', err);
    });
}, [userId]);



  const handleChange = (e) => {
    setUserInfo({ ...userInfo, [e.target.name]: e.target.value });
  };

  const toggleEdit = () => {
    setIsEditing(!isEditing);
  };

  const saveChanges = () => {
    fetch(`http://localhost:8081/user/${userId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(userInfo),
    }).then(res => {
      if (res.ok) {
        setIsEditing(false);
      } else {
        alert('Failed to save changes');
      }
    });
  };

  if (!userInfo) return <p>Loading...</p>;

  return (
    <div style={{ border: '1px solid #ccc', padding: '15px', borderRadius: '8px' }}>
      <h3>Billing Information</h3>
      <div>
        <label>First Name:</label>
        <input
          name="firstName"
          value={userInfo.firstName}
          onChange={handleChange}
          disabled={!isEditing}
        />
      </div>
      <div>
        <label>Last Name:</label>
        <input
          name="lastName"
          value={userInfo.lastName}
          onChange={handleChange}
          disabled={!isEditing}
        />
      </div>
      <div>
        <label>Mobile No:</label>
        <input
          name="mobileNo"
          value={userInfo.mobileNo}
          onChange={handleChange}
          disabled={!isEditing}
        />
      </div>
      <div>
        <label>Email:</label>
        <input
          name="email"
          value={userInfo.email}
          onChange={handleChange}
          disabled={!isEditing}
        />
      </div>
      <div>
        <label>Address:</label>
        <input
          name="address"
          value={userInfo.address}
          onChange={handleChange}
          disabled={!isEditing}
        />
      </div>
      <div style={{ marginTop: '10px' }}>
        {isEditing ? (
          <>
            <button onClick={saveChanges}>Save</button>
            <button onClick={toggleEdit} style={{ marginLeft: '10px' }}>Cancel</button>
          </>
        ) : (
          <button onClick={toggleEdit}>Edit</button>
        )}
      </div>
    </div>
  );
};

export default BillingInfo;