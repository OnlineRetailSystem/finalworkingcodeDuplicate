package com.ecom.authservice.controllers;

import com.ecom.authservice.kafka.AuthEventProducer;
import com.ecom.authservice.models.User;
import com.ecom.authservice.repositories.UserRepository;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

@RestController
public class AuthController {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private PasswordEncoder passwordEncoder;

    @Autowired
    private AuthEventProducer authEventProducer;

    @PostMapping("/signup")
    public ResponseEntity<String> signup(@RequestBody User user) {
        if (userRepository.findByUsername(user.getUsername()).isPresent()) {
            return ResponseEntity.badRequest().body("Username already exists");
        }
        user.setPassword(passwordEncoder.encode(user.getPassword()));
        user.setRoles("ROLE_USER");
        User savedUser = userRepository.save(user);

        // Publish USER_REGISTERED event to Kafka
        authEventProducer.publishUserRegistered(
                savedUser.getUsername(),
                savedUser.getEmail(),
                savedUser.getRoles()
        );

        return ResponseEntity.ok("User registered successfully");
    }

    @PostMapping("/signin")
    public ResponseEntity<String> signin(@RequestBody User user) {
        Optional<User> existingUser = userRepository.findByUsername(user.getUsername());
        if (existingUser.isPresent()) {
            boolean matches = passwordEncoder.matches(user.getPassword(), existingUser.get().getPassword());
            if (matches) {
                // Publish USER_LOGGED_IN event to Kafka
                authEventProducer.publishUserLoggedIn(
                        existingUser.get().getUsername(),
                        existingUser.get().getEmail(),
                        existingUser.get().getRoles()
                );
                return ResponseEntity.ok("Login successful");
            }
        }
        return ResponseEntity.status(401).body("Invalid credentials");
    }

    @GetMapping("/user/{username}")
    public ResponseEntity<User> getUser(@PathVariable String username) {
        Optional<User> userOpt = userRepository.findByUsername(username);
        if (userOpt.isPresent()) {
            User user = userOpt.get();
            user.setPassword(null); // hide password
            return ResponseEntity.ok(user);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @PutMapping("/user/{username}")
    public ResponseEntity<String> updateUser(@PathVariable String username, @RequestBody User updatedUser) {
        Optional<User> userOpt = userRepository.findByUsername(username);
        if (userOpt.isPresent()) {
            User user = userOpt.get();
            if (updatedUser.getEmail() != null) {
                user.setEmail(updatedUser.getEmail());
            }
            if (updatedUser.getRoles() != null) {
                user.setRoles(updatedUser.getRoles());
            }
            if (updatedUser.getFirstName() != null) {
                user.setFirstName(updatedUser.getFirstName());
            }
            if (updatedUser.getLastName() != null) {
                user.setLastName(updatedUser.getLastName());
            }
            if (updatedUser.getAddress() != null) {
                user.setAddress(updatedUser.getAddress());
            }
            if (updatedUser.getMobileNo() != null) {
                user.setMobileNo(updatedUser.getMobileNo());
            }
            userRepository.save(user);
            return ResponseEntity.ok("User updated successfully");
        } else {
            return ResponseEntity.notFound().build();
        }
    }
}