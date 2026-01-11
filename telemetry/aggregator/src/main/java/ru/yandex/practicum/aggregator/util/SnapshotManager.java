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

        // ВСЕГДА обновляем timestamp снапшота
        snapshot.setTimestamp(event.getTimestamp());

        boolean shouldUpdate = false;

        if (oldState == null) {
            shouldUpdate = true; // новый датчик
        } else if (event.getTimestamp() > oldState.getTimestamp()) {
            shouldUpdate = true; // новое событие по времени
        } else if (event.getTimestamp() == oldState.getTimestamp()) {
            // Событие с тем же timestamp — проверяем данные
            if (!event.getPayload().equals(oldState.getData())) {
                shouldUpdate = true; // данные изменились
            }
        }

        if (shouldUpdate) {
            SensorStateAvro newState = SensorStateAvro.newBuilder()
                    .setTimestamp(event.getTimestamp())
                    .setData(event.getPayload())
                    .build();
            sensors.put(sensorId, newState);
        }

        return Optional.of(snapshot);
    }
}