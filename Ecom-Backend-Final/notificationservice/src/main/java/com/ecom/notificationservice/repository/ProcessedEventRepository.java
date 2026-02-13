package com.ecom.notificationservice.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.ecom.notificationservice.model.ProcessedEvent;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
    boolean existsByEventId(String eventId);
}
