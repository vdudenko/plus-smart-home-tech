package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.converter.HubEventConverter;
import ru.yandex.practicum.collector.converter.SensorEventConverter;
import ru.yandex.practicum.collector.model.HubEvent;
import ru.yandex.practicum.collector.model.SensorEvent;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Service
@RequiredArgsConstructor
public class CollectorService {

    private final KafkaTemplate<String, byte[]> hubKafkaTemplate;
    private final KafkaTemplate<String, byte[]> sensorKafkaTemplate;

    public void handleHubEvent(HubEvent event) {
        HubEventAvro avro = HubEventConverter.toAvro(event);
        byte[] data = serializeAvro(avro);
        hubKafkaTemplate.send("telemetry.hubs.v1", event.getHubId(), data);
    }

    public void handleSensorEvent(SensorEvent event) {
        SensorEventAvro avro = SensorEventConverter.toAvro(event);
        byte[] data = serializeAvro(avro);
        sensorKafkaTemplate.send("telemetry.sensors.v1", event.getId(), data);
    }

    private byte[] serializeAvro(SpecificRecord record) {
        SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(record.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            writer.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Avro", e);
        }
        return out.toByteArray();
    }
}