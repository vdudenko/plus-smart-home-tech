package ru.yandex.practicum.aggregator.util;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class SnapshotManager {

    // 🔑 КРИТИЧЕСКИ ВАЖНО: состояние НЕ хранится между событиями!
    // Каждый снапшот строится "с нуля" на основе текущего события + предыдущего состояния в памяти
    private final Map<String, Map<String, SensorStateAvro>> hubStates = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();
        long eventTimestamp = event.getTimestamp();

        // Получаем или создаём состояние хаба
        Map<String, SensorStateAvro> hubState = hubStates.computeIfAbsent(hubId, k -> new HashMap<>());

        // Получаем текущее состояние сенсора
        SensorStateAvro oldState = hubState.get(sensorId);

        // Игнорируем события с меньшим таймстампом (защита от дубликатов/задержек)
        if (oldState != null && eventTimestamp < oldState.getTimestamp()) {
            return Optional.empty();
        }

        // 🔑 ПРАВИЛЬНО: создаём НОВОЕ состояние сенсора на основе события
        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(eventTimestamp)
                .setData(event.getPayload())  // payload уже правильного типа для union
                .build();

        // Обновляем состояние сенсора в хабе
        hubState.put(sensorId, newState);

        // 🔑 СОЗДАЁМ НОВЫЙ СНАПШОТ "С НУЛЯ" (без копирования старого объекта!)
        SensorsSnapshotAvro snapshot = SensorsSnapshotAvro.newBuilder()
                .setHubId(hubId)
                .setTimestamp(eventTimestamp)
                .setSensorsState(new HashMap<>(hubState))  // копия текущего состояния
                .build();

        return Optional.of(snapshot);
    }

    // 🔑 ОЧИСТКА СОСТОЯНИЯ (вызывается извне при необходимости)
    public void clear() {
        hubStates.clear();
    }
}