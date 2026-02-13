package com.ecom.authservice.events;

import java.time.LocalDateTime;
import java.util.UUID;

public class UserEvent {

    private String eventId;
    private String eventType; // USER_REGISTERED or USER_LOGGED_IN
    private String username;
    private String email;
    private String roles;
    private LocalDateTime timestamp;

    public UserEvent() {
    }

    public UserEvent(String eventType, String username, String email, String roles) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.username = username;
        this.email = email;
        this.roles = roles;
        this.timestamp = LocalDateTime.now();
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
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

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserEvent{" +
                "eventId='" + eventId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", username='" + username + '\'' +
                ", email='" + email + '\'' +
                ", roles='" + roles + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
