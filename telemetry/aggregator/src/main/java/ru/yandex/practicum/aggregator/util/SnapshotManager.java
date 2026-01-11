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

        boolean shouldUpdate = false;

        if (oldState == null) {
            shouldUpdate = true;
        } else if (oldState.getTimestamp() < event.getTimestamp()) {
            if (!oldState.getData().equals(event.getPayload())) {
                shouldUpdate = true;
            }
        }
        snapshot.setTimestamp(event.getTimestamp());

        if (shouldUpdate) {
            SensorStateAvro newState = SensorStateAvro.newBuilder()
                    .setTimestamp(event.getTimestamp())
                    .setData(event.getPayload())
                    .build();
            sensors.put(sensorId, newState);
            return Optional.of(snapshot);
        } else {
            return Optional.of(snapshot);
        }
    }
}