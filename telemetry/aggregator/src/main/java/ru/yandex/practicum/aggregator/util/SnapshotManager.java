package ru.yandex.practicum.aggregator.util;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class SnapshotManager {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();

        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId,
                id -> SensorsSnapshotAvro.newBuilder()
                        .setHubId(id)
                        .setTimestamp(event.getTimestamp())
                        .setSensorsState(new HashMap<>())
                        .build());

        Map<String, SensorStateAvro> sensors = snapshot.getSensorsState();
        SensorStateAvro oldState = sensors.get(sensorId);

        if (oldState != null) {
            if (oldState.getTimestamp() >= event.getTimestamp()) {
                return Optional.empty(); // устаревшее событие
            }
            if (oldState.getData().equals(event.getPayload())) {
                return Optional.empty(); // данные не изменились
            }
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        sensors.put(sensorId, newState);
        snapshot.setTimestamp(event.getTimestamp());

        return Optional.of(snapshot);
    }
}