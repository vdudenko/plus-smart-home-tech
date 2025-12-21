package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.converter.HubEventConverter;
import ru.yandex.practicum.collector.converter.SensorEventConverter;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import smart_home.collector.v1.CollectHubEventRequest;
import smart_home.collector.v1.CollectSensorEventRequest;

@Service
@RequiredArgsConstructor
public class CollectorService {

    private final KafkaTemplate<String, HubEventAvro> hubKafkaTemplate;
    private final KafkaTemplate<String, SensorEventAvro> sensorKafkaTemplate;

    public void handleHubEvent(CollectHubEventRequest request) {
        HubEventAvro avro = HubEventConverter.toAvro(request);
        hubKafkaTemplate.send("telemetry.hubs.v1", avro.getHubId(), avro);
    }

    public void handleSensorEvent(CollectSensorEventRequest request) {
        SensorEventAvro avro = SensorEventConverter.toAvro(request);
        sensorKafkaTemplate.send("telemetry.sensors.v1", avro.getId(), avro);
    }
}
