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
            // Сравниваем данные по типам датчиков
            shouldUpdate = !arePayloadsEqual(event.getPayload(), oldState.getData());
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

    private boolean arePayloadsEqual(Object newPayload, Object oldPayload) {
        if (newPayload == null && oldPayload == null) return true;
        if (newPayload == null || oldPayload == null) return false;

        // ClimateSensorAvro
        if (newPayload instanceof ClimateSensorAvro newClimate && oldPayload instanceof ClimateSensorAvro oldClimate) {
            return newClimate.getTemperatureC() == oldClimate.getTemperatureC() &&
                   newClimate.getHumidity() == oldClimate.getHumidity() &&
                   newClimate.getCo2Level() == oldClimate.getCo2Level();
        }
        // LightSensorAvro
        if (newPayload instanceof LightSensorAvro newLight && oldPayload instanceof LightSensorAvro oldLight) {
            return newLight.getLuminosity() == oldLight.getLuminosity() &&
                   newLight.getLinkQuality() == oldLight.getLinkQuality();
        }
        // MotionSensorAvro
        if (newPayload instanceof MotionSensorAvro newMotion && oldPayload instanceof MotionSensorAvro oldMotion) {
            return newMotion.getMotion() == oldMotion.getMotion() &&
                   newMotion.getLinkQuality() == oldMotion.getLinkQuality() &&
                   newMotion.getVoltage() == oldMotion.getVoltage();
        }
        // SwitchSensorAvro
        if (newPayload instanceof SwitchSensorAvro newSwitch && oldPayload instanceof SwitchSensorAvro oldSwitch) {
            return newSwitch.getState() == oldSwitch.getState();
        }
        // TemperatureSensorAvro
        if (newPayload instanceof TemperatureSensorAvro newTemp && oldPayload instanceof TemperatureSensorAvro oldTemp) {
            return newTemp.getTemperatureC() == oldTemp.getTemperatureC() &&
                   newTemp.getTemperatureF() == oldTemp.getTemperatureF();
        }

        return false;
    }
}