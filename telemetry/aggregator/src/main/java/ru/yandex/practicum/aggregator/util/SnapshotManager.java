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
                        .setTimestamp(0L)
                        .setSensorsState(new HashMap<>())
                        .build());

        Map<String, SensorStateAvro> sensors = snapshot.getSensorsState();
        SensorStateAvro oldState = sensors.get(sensorId);

        // Проверка на необходимость обновления
        if (oldState != null) {
            if (event.getTimestamp() < oldState.getTimestamp()) {
                return Optional.empty(); // устаревшее
            }
            if (event.getTimestamp() == oldState.getTimestamp() &&
                    event.getPayload().equals(oldState.getData())) {
                return Optional.empty(); // дубликат
            }
        }

        // Создаём новое состояние
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();
        sensors.put(sensorId, newState);

        // ВСЕГДА обновляем timestamp снапшота до timestamp события
        snapshot.setTimestamp(event.getTimestamp()); // ← УБРАТЬ УСЛОВИЕ!

        return Optional.of(SensorsSnapshotAvro.newBuilder(snapshot).build());
    }
}