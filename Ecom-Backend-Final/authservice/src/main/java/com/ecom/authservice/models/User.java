package com.ecom.authservice.models;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data //creates all getter and setter, provide constructor for req field
@NoArgsConstructor //generates default constructor
@AllArgsConstructor //generates a constructor with parameters for all class fields
@Table(name = "users")
public class User {
	
    @Id //specifies the primary key of the entity
    @GeneratedValue(strategy = GenerationType.IDENTITY) //specifies how primary key is generated 
    //The IDENTITY strategy indicates that the database auto-increments the primary key value when new records are inserted.
    private Long id;
    
    @Column(unique = true)
    private String username;
    private String password;
    private String email; 
    public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getRoles() {
		return roles;
	}
	public void setRoles(String roles) {
		this.roles = roles;
	}
	public String getFirstName() {
		return firstName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getAddress() {
		return address;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public String getMobileNo() {
		return mobileNo;
	}
	public void setMobileNo(String mobileNo) {
		this.mobileNo = mobileNo;
	}
	private String roles;
    private String firstName;
    private String lastName;
    private String address;
    private String mobileNo;
}