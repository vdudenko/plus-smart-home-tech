package ru.yandex.practicum.analyzer.service;

import lombok.AllArgsConstructor;
import lombok.Getter;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

@Getter
@AllArgsConstructor
public class DeferredScenario {
    private final HubEventAvro event;
    private final long timestamp;
}