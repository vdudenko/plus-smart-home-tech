package ru.yandex.practicum.aggregator.util;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class SnapshotManager {

    private final Map<String, Map<String, SensorStateAvro>> states = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();
        long timestamp = event.getTimestamp();

        Map<String, SensorStateAvro> hubState = states.computeIfAbsent(hubId, k -> new HashMap<>());

        SensorStateAvro oldState = hubState.get(sensorId);
        if (oldState != null && timestamp < oldState.getTimestamp()) {
            return Optional.empty(); // Игнорируем ТОЛЬКО старые события
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(timestamp)
                .setData(event.getPayload())  // payload уже правильного типа для union
                .build();

        hubState.put(sensorId, newState);

        SensorsSnapshotAvro snapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(timestamp)
                .setSensorsState(new HashMap<>(hubState))  // Глубокая копия
                .build();

        return Optional.of(snapshot);
    }

    public void clear() {
        states.clear();
    }
}