package com.ecom.notificationservice.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ecom.notificationservice.model.ProcessedEvent;
import com.ecom.notificationservice.repository.ProcessedEventRepository;

@RestController
@RequestMapping("/notifications")
public class NotificationController {

    private final ProcessedEventRepository processedEventRepository;

    public NotificationController(ProcessedEventRepository processedEventRepository) {
        this.processedEventRepository = processedEventRepository;
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Notification Service is running");
    }

    @GetMapping("/processed-events")
    public ResponseEntity<List<ProcessedEvent>> getProcessedEvents() {
        return ResponseEntity.ok(processedEventRepository.findAll());
    }
}
