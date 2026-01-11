package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.converter.HubEventProtoConverter;
import ru.yandex.practicum.collector.converter.SensorEventProtoConverter;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Service
@RequiredArgsConstructor
public class CollectorService {

    private final KafkaTemplate<String, HubEventAvro> hubKafkaTemplate;
    private final KafkaTemplate<String, SensorEventAvro> sensorKafkaTemplate;
    private final HubEventProtoConverter hubEventConverter;
    private final SensorEventProtoConverter sensorEventConverter;

    public void handleHubEvent(HubEventProto event) {
        HubEventAvro avro = hubEventConverter.toAvro(event);
        hubKafkaTemplate.send("telemetry.hubs.v1", avro.getHubId(), avro);
    }

    public void handleSensorEvent(SensorEventProto event) {
        SensorEventAvro avro = sensorEventConverter.toAvro(event);
        sensorKafkaTemplate.send("telemetry.sensors.v1", avro.getId(), avro);
    }
}