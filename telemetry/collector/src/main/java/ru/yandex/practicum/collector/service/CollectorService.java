package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.converter.HubEventProtoConverter;
import ru.yandex.practicum.collector.converter.SensorEventProtoConverter;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Service
@RequiredArgsConstructor
public class CollectorService {

    private final KafkaTemplate<String, byte[]> hubKafkaTemplate;
    private final KafkaTemplate<String, byte[]> sensorKafkaTemplate;
    private final HubEventProtoConverter hubEventConverter;
    private final SensorEventProtoConverter sensorEventConverter;

    public void handleHubEvent(HubEventProto event) {
        HubEventAvro avro = hubEventConverter.toAvro(event);
        byte[] data = serializeAvro(avro);
        hubKafkaTemplate.send("telemetry.hubs.v1", avro.getHubId(), data);
    }

    public void handleSensorEvent(SensorEventProto event) {
        SensorEventAvro avro = sensorEventConverter.toAvro(event);
        byte[] data = serializeAvro(avro);
        sensorKafkaTemplate.send("telemetry.sensors.v1", avro.getId(), data);
    }

    private byte[] serializeAvro(SpecificRecord avro) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(avro.getSchema());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(avro, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Avro", e);
        }
    }
}