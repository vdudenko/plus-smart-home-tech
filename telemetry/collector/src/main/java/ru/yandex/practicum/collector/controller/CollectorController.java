package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.collector.model.HubEvent;
import ru.yandex.practicum.collector.model.SensorEvent;
import ru.yandex.practicum.collector.service.CollectorService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/events")
public class CollectorController {
    private final CollectorService collectorService;

    @PostMapping("/sensors")
    public ResponseEntity<Void> collectSensor(@Valid @RequestBody SensorEvent event) {
        collectorService.handleSensorEvent(event);
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/hubs")
    public ResponseEntity<Void> collectHub(@Valid @RequestBody HubEvent event) {
        collectorService.handleHubEvent(event);
        return ResponseEntity.accepted().build();
    }
}
