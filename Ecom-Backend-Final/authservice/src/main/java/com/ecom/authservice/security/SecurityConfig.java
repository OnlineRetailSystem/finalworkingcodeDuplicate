package com.ecom.authservice.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.ecom.authservice.services.CustomUserDetailsService;

@Configuration
public class SecurityConfig {

    // PasswordEncoder Bean
    @Bean //beans are objects managed by spring container, singleton scoped by default, meaning only one instance exists in the Spring context per application.
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    // UserDetailsService Bean
    @Bean
    public UserDetailsService userDetailsService() {
        return new CustomUserDetailsService();
    }

    // CORS Configuration
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**") // Apply to all endpoints
                        .allowedOrigins("http://localhost:5173")
                        .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                        .allowedHeaders("*");
            }
        };
    }

    // Spring Security Filter Chain Configuration
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
            // Enable CORS
            .cors(Customizer.withDefaults())
            
            // Disable CSRF for API
            .csrf(csrf -> csrf.disable()) //csrf -- cross site request forgery -- security attack where a malicious
            //website tricks a user's browser into making unwanted actions on another website where the user is authenticated.
            //CSRF is disabled to simplify API security
            
            // Configure endpoints that are public
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/signup", "/signin", "/h2-console/**").permitAll()
                .anyRequest().authenticated()
            )
            // Enable HTTP Basic authentication
            .httpBasic(Customizer.withDefaults())
            // Configure headers for H2 console if needed
            .headers(headers -> headers.frameOptions(frame -> frame.sameOrigin()));
        return http.build();
    }
}