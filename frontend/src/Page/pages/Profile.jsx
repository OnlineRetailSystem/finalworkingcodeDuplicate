import React, { useState, useEffect } from 'react';
import axios from 'axios';

const Profile = () => {

  const url = `http://localhost:8081/user/${username}`;

  
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [isEditing, setIsEditing] = useState(false);
  const [formData, setFormData] = useState({
    username: '',
    email: '',
    firstName: '',
    lastName: '',
    mobileNo: '',
    address: ''
  });

  // Fetch user data on component load
  useEffect(() => {
    axios
      .get(url)
      .then((res) => {
        setUser(res.data);
        setFormData({
          username: res.data.username || '',
          email: res.data.email || '',
          firstName: res.data.firstName || '',
          lastName: res.data.lastName || '',
          mobileNo: res.data.mobileNo || '',
          address: res.data.address || ''
        });
        setLoading(false);
      })
      .catch((error) => {
        console.error('Error fetching user:', error);
        alert('Error fetching user data. Make sure your backend is running and CORS is configured.');
        setLoading(false);
      });
  }, [url]);

  const handleChange = (e) => {
    setFormData((prev) => ({ ...prev, [e.target.name]: e.target.value }));
  };

  const handleEdit = () => {
    setIsEditing(true);
  };

  const handleSave = () => {
  const updatedUser = {
    ...user,
    username: formData.username,
    email: formData.email,
    firstName: formData.firstName,
    lastName: formData.lastName,
    mobileNo: formData.mobileNo,
    address: formData.address
  };

  axios
    .put(url, updatedUser)
    .then((res) => {
      // Now res.data is the full, updated user object.
      setUser(res.data); // update local state to reflect changes immediately
      setIsEditing(false);
    })
    .catch((err) => {
      console.error("Update error:", err);
      alert("Failed to update");
    });
};

  if (loading) {
    return (
      <div className="profile-container">
        <div className="profile-box">
          <div className="loader">Loading...</div>
        </div>
      </div>
    );
  }

  if (!user) {
    return (
      <div className="profile-container">
        <div className="profile-box">
          <p>No user data available.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="profile-container">
      <div className="profile-box">
        {/* Profile Header */}
        <div className="profile-header">
          <img
            src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRjKU8YDosyoTjWVSrMGvkVLFbrx2Xyn4qPrg&s"
            alt="Profile"
            className="profile-image"
          />
          {/* Removed margin to close the gap between image and details */}
          <h2 className="profile-username">{user.firstName}</h2>
        </div>

        {/* Mode: Edit or View */}
        {isEditing ? (
          <div className="profile-form">
            {/* Each field in one line */}
            <div className="form-group">
              <label>Username:</label>
              <input
                type="text"
                name="username"
                value={formData.username}
                onChange={handleChange}
              />
            </div>
            <div className="form-group">
              <label>Email:</label>
              <input
                type="email"
                name="email"
                value={formData.email}
                onChange={handleChange}
              />
            </div>
            <div className="form-group">
              <label>First Name:</label>
              <input
                type="text"
                name="firstName"
                value={formData.firstName}
                onChange={handleChange}
              />
            </div>
            <div className="form-group">
              <label>Last Name:</label>
              <input
                type="text"
                name="lastName"
                value={formData.lastName}
                onChange={handleChange}
              />
            </div>
            <div className="form-group">
              <label>Mobile Number:</label>
              <input
                type="tel"
                name="mobileNo"
                value={formData.mobileNo}
                onChange={handleChange}
              />
            </div>
            <div className="form-group">
              <label>Address:</label>
              <input
                type="text"
                name="address"
                value={formData.address}
                onChange={handleChange}
              />
            </div>
            <button className="save-button" onClick={handleSave}>Save</button>
          </div>
        ) : (
          <div className="profile-info">
            <p>
              <strong>Full Name:</strong> {user.firstName} {user.lastName}
            </p>
            <p>
              <strong>Email:</strong> {user.email}
            </p>
            <p>
              <strong>Mobile Number:</strong> {user.mobileNo}
            </p>
            <p>
              <strong>Address:</strong> {user.address}
            </p>
            <button className="edit-button" onClick={handleEdit}>Edit</button>
          </div>
        )}
      </div>

      {/* CSS styles inline for demo; you can move these to a stylesheet */}
      <style jsx>{`
        .profile-container {
          display: flex;
          justify-content: center;
          padding: 30px 20px;
          background-color: #f0f2f5;
          min-height: 100vh;
        }
        .profile-box {
          background: #fff;
          padding: 30px;
          max-width: 700px;
          width: 100%;
          border-radius: 15px;
          box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
          transition: all 0.3s ease;
          text-align: center; /* center all content */
        }
        @media(max-width: 640px) {
          .profile-box {
            padding: 20px;
          }
        }
        /* Header: image and title in center, no gap */
        .profile-header {
          display: flex;
          flex-direction: column;
          align-items: center;
          margin-bottom: 20px;
        }
        .profile-image {
          border-radius: 50%;
          width: 200px;
          height: 200px;
        }
        /* Center title below image with no margin between image and title */
        .profile-username {
          margin-top: 10px;
          font-size: 1.5rem;
        }
        /* Info display, left aligned, no gap between label and data */
        .profile-info {
          width: 100%;
          max-width: 600px;
          margin: 0 auto;
          text-align: left; /* Left align text in info display */
        }
        .profile-info p {
          display: flex;
          justify-content: flex-start;
          align-items: center;
          gap: 8px;
          margin: 10px 0;
        }
        /* Form inline: label with fixed width, inputs flexible */
        .profile-form {
          display: flex;
          flex-direction: column;
          align-items: center; /* center the form horizontally */
        }
        .form-group {
          display: flex;
          align-items: center;
          justify-content: flex-start;
          gap: 10px;
          width: 100%;
          max-width: 600px;
          margin-bottom: 15px;
          flex-wrap: wrap;
        }
        label {
          width: 150px;
          flex-shrink: 0;
          font-weight: 600;
          text-align: right; /* labels right aligned for cleaner look */
        }
        input {
          flex: 1;
          padding: 10px;
          border: 1px solid #ccc;
          border-radius: 8px;
          font-size: 1rem;
          width: 100%;
        }
        /* Buttons styling */
        .edit-button, .save-button {
          padding: 10px 20px;
          border: none;
          border-radius: 8px;
          cursor: pointer;
          font-size: 1rem;
          transition: background-color 0.2s;
          margin-top: 10px;
        }
        .edit-button {
          background-color: #ff69b4;
          color: #fff;
        }
        .edit-button:hover {
          background-color: #ff85c1;
        }
        .save-button {
          background-color: #ff69b4;
          color: #fff;
        }
        .save-button:hover {
          background-color: #ff85c1;
        }
        /* Loader styling */
        .loader {
          font-size: 1.5rem;
          text-align: center;
          padding: 40px 0;
        }
      `}</style>
    </div>
  );
};

export default Profile;