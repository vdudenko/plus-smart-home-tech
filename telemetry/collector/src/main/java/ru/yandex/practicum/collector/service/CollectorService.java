package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.converter.HubEventConverter;
import ru.yandex.practicum.collector.converter.SensorEventConverter;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import telemetry.service.collector.CollectHubEventRequest;
import telemetry.service.collector.CollectSensorEventRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Service
@RequiredArgsConstructor
public class CollectorService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public void handleHubEvent(CollectHubEventRequest request) {
        HubEventAvro avro = HubEventConverter.toAvro(request);
        byte[] data = serializeAvro(avro);
        kafkaTemplate.send("telemetry.hubs.v1", avro.getHubId(), data);
    }

    public void handleSensorEvent(CollectSensorEventRequest request) {
        SensorEventAvro avro = SensorEventConverter.toAvro(request);
        byte[] data = serializeAvro(avro);
        kafkaTemplate.send("telemetry.sensors.v1", avro.getId(), data);
    }

    private byte[] serializeAvro(SpecificRecordBase record) {
        DatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>(record.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<SpecificRecordBase> dataFileWriter = new DataFileWriter<>(writer)) {
            dataFileWriter.create(record.getSchema(), out);
            dataFileWriter.append(record);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Avro", e);
        }
        return out.toByteArray();
    }
}
