package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.analyzer.entity.Action;
import ru.yandex.practicum.analyzer.entity.Condition;
import ru.yandex.practicum.analyzer.entity.Scenario;
import ru.yandex.practicum.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioEvaluator {

    public List<DeviceActionAvro> evaluate(
            SensorsSnapshotAvro snapshot,
            List<Scenario> scenarios) {

        List<DeviceActionAvro> actions = new ArrayList<>();

        for (Scenario scenario : scenarios) {
            if (isScenarioFulfilled(scenario, snapshot)) {
                actions.addAll(scenario.getActions().stream()
                        .map(ScenarioAction::getAction)  // ← Получаем Action
                        .map(this::toAvroAction)
                        .toList());
            }
        }
        return actions;
    }

    private boolean isScenarioFulfilled(Scenario scenario, SensorsSnapshotAvro snapshot) {
        Map<String, SensorStateAvro> states = snapshot.getSensorsState();

        return scenario.getConditions().stream().allMatch(scenarioCondition -> {
            String sensorId = scenarioCondition.getSensorId();  // ← Берём из ScenarioCondition
            SensorStateAvro state = states.get(sensorId);
            if (state == null) return false;
            return checkCondition(scenarioCondition.getCondition(), state);  // ← Передаём Condition
        });
    }

    private String getSensorIdForCondition(Condition condition) {
        throw new UnsupportedOperationException("Implement condition-to-sensor mapping");
    }

    private boolean checkCondition(Condition condition, SensorStateAvro state) {
        Object data = state.getData();
        int actualValue = extractValue(data, condition.getType());
        int expectedValue = condition.getValue();

        return switch (condition.getOperation()) {
            case "EQUALS" -> actualValue == expectedValue;
            case "GREATER_THAN" -> actualValue > expectedValue;
            case "LOWER_THAN" -> actualValue < expectedValue;
            default -> false;
        };
    }

    private int extractValue(Object data, String type) {
        return switch (type) {
            case "MOTION", "SWITCH" -> ((SwitchSensorAvro) data).getState() ? 1 : 0;
            case "LUMINOSITY" -> ((LightSensorAvro) data).getLuminosity();
            case "TEMPERATURE" -> ((ClimateSensorAvro) data).getTemperatureC();
            case "HUMIDITY" -> ((ClimateSensorAvro) data).getHumidity();
            case "CO2LEVEL" -> ((ClimateSensorAvro) data).getCo2Level();
            default -> 0;
        };
    }

    private DeviceActionAvro toAvroAction(Action action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())  // ← Добавьте это поле в Action!
                .setType(toAvroActionType(action.getType()))
                .setValue(action.getValue())
                .build();
    }

    private ActionTypeAvro toAvroActionType(String type) {
        return switch (type) {
            case "ACTIVATE" -> ActionTypeAvro.ACTIVATE;
            case "DEACTIVATE" -> ActionTypeAvro.DEACTIVATE;
            case "INVERSE" -> ActionTypeAvro.INVERSE;
            case "SET_VALUE" -> ActionTypeAvro.SET_VALUE;
            default -> throw new IllegalArgumentException("Unknown action type: " + type);
        };
    }
}