package com.ecom.authservice.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.ecom.authservice.events.UserEvent;

@Service
public class AuthEventProducer {

    private static final Logger log = LoggerFactory.getLogger(AuthEventProducer.class);

    private static final String TOPIC_USER_REGISTERED = "USER_REGISTERED";
    private static final String TOPIC_USER_LOGGED_IN = "USER_LOGGED_IN";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public AuthEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishUserRegistered(String username, String email, String roles) {
        UserEvent event = new UserEvent("USER_REGISTERED", username, email, roles);
        log.info("Publishing USER_REGISTERED event: {}", event);
        kafkaTemplate.send(TOPIC_USER_REGISTERED, event.getEventId(), event);
    }

    public void publishUserLoggedIn(String username, String email, String roles) {
        UserEvent event = new UserEvent("USER_LOGGED_IN", username, email, roles);
        log.info("Publishing USER_LOGGED_IN event: {}", event);
        kafkaTemplate.send(TOPIC_USER_LOGGED_IN, event.getEventId(), event);
    }
}
