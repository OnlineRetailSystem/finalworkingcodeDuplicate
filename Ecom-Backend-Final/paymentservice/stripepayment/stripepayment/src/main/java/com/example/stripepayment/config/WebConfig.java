package com.example.stripepayment.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.stripe.Stripe;

import jakarta.annotation.PostConstruct;

@Configuration
public class WebConfig {

    @Value("${stripe.apiKey}")
    private String apiKey;

    @PostConstruct
    public void setup() {
        Stripe.apiKey = apiKey;
        System.out.println("Stripe initialized with API key");
    }

    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
           @Override
           public void addCorsMappings(CorsRegistry registry) {
               registry.addMapping("/**")
                       .allowedOrigins("http://localhost:5173")
                       .allowedMethods("GET", "POST", "PUT", "DELETE")
                       .allowedHeaders("*");
           }
        };
    }
}