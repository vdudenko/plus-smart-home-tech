package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.converter.HubEventConverter;
import ru.yandex.practicum.collector.converter.SensorEventConverter;
import ru.yandex.practicum.collector.model.HubEvent;
import ru.yandex.practicum.collector.model.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Service
@RequiredArgsConstructor
public class CollectorService {

    private final KafkaTemplate<String, HubEventAvro> hubKafkaTemplate;
    private final KafkaTemplate<String, SensorEventAvro> sensorKafkaTemplate;

    public void handleHubEvent(HubEvent event) {
        HubEventAvro avro = HubEventConverter.toAvro(event); // ← Новый конвертер из JSON-model → Avro
        hubKafkaTemplate.send("telemetry.hubs.v1", event.getHubId(), avro);
    }

    public void handleSensorEvent(SensorEvent event) {
        SensorEventAvro avro = SensorEventConverter.toAvro(event);
        sensorKafkaTemplate.send("telemetry.sensors.v1", event.getId(), avro);
    }
}
