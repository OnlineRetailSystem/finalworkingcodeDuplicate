package com.ecom.authservice.services;

import com.ecom.authservice.models.User;
import com.ecom.authservice.repositories.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import java.util.Arrays;
import java.util.stream.Collectors;

//This class implements Spring Security's UserDetailsService interface, which is a core component
//in configuring authentication in Spring Security.
//It fetches user data from your database (via UserRepository) based on the username provided during login.
public class CustomUserDetailsService implements UserDetailsService {

    @Autowired
    private UserRepository userRepository;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        User user = userRepository.findByUsername(username)
                      .orElseThrow(() -> new UsernameNotFoundException("User not found"));

        return new org.springframework.security.core.userdetails.User(
                user.getUsername(),
                user.getPassword(),
                Arrays.stream(user.getRoles().split(","))
                      .map(role -> new SimpleGrantedAuthority(role.trim()))
                      .collect(Collectors.toList())
        );
    }
}