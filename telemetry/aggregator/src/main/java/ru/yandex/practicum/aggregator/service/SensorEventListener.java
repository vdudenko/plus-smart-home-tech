package ru.yandex.practicum.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.aggregator.util.SnapshotManager;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorEventListener {

    private final SnapshotManager snapshotManager;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    @KafkaListener(topics = "telemetry.sensors.v1", groupId = "aggregator-group-${random.uuid}")
    public void handleSensorEvent(SensorEventAvro event) {
        try {
            snapshotManager.updateState(event).ifPresent(snapshot -> {
                byte[] data = serializeAvro(snapshot);

                kafkaTemplate.send("telemetry.snapshots.v1", snapshot.getHubId(), data)
                        .get(1, TimeUnit.SECONDS);

                log.info("SNAPSHOT_PUBLISHED hub={} devices={}",
                        snapshot.getHubId(), snapshot.getSensorsState().size());
            });
        } catch (Exception e) {
            log.error("ERROR processing event: hubId={}, deviceId={}",
                    event.getHubId(), event.getId(), e);
        }
    }

    private byte[] serializeAvro(SpecificRecord avro) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(avro.getSchema());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(avro, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed for hub: " +
                    ((SensorsSnapshotAvro) avro).getHubId(), e);
        }
    }
}