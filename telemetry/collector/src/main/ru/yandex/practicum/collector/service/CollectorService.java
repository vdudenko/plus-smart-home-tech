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

    private final KafkaTemplate<String, SensorEventAvro> sensorKafkaTemplate;
    private final KafkaTemplate<String, HubEventAvro> hubKafkaTemplate;

    public void handleSensorEvent(SensorEvent event) {
        SensorEventAvro avro = SensorEventConverter.toAvro(event);
        sensorKafkaTemplate.send("telemetry.sensors.v1", avro.getId(), avro);
    }

    public void handleHubEvent(HubEvent event) {
        HubEventAvro avro = HubEventConverter.toAvro(event);
        hubKafkaTemplate.send("telemetry.hubs.v1", avro.getHubId(), avro);
    }
}
