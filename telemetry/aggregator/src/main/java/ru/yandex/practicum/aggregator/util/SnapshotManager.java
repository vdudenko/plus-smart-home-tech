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

        // Гарантируем, что работаем только с снапшотом текущего хаба
        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId,
                id -> SensorsSnapshotAvro.newBuilder()
                        .setHubId(id)
                        .setTimestamp(event.getTimestamp())
                        .setSensorsState(new HashMap<>())
                        .build());

        // Обновляем timestamp снапшота ТОЛЬКО если событие новее
        if (event.getTimestamp() > snapshot.getTimestamp()) {
            snapshot.setTimestamp(event.getTimestamp());
        }

        Map<String, SensorStateAvro> sensors = snapshot.getSensorsState();
        SensorStateAvro oldState = sensors.get(sensorId);

        boolean shouldUpdate = false;

        if (oldState == null) {
            shouldUpdate = true; // новый датчик
        } else if (event.getTimestamp() > oldState.getTimestamp()) {
            shouldUpdate = true; // новое событие по времени
        } else if (event.getTimestamp() == oldState.getTimestamp()) {
            // Для Avro-объектов equals() может не работать — сравниваем через toString()
            if (!event.getPayload().toString().equals(oldState.getData().toString())) {
                shouldUpdate = true;
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