package ru.yandex.practicum.analyzer.service;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.analyzer.entity.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioEvaluator {

    public List<DeviceActionAvro> evaluate(SensorsSnapshotAvro snapshot, List<Scenario> scenarios) {
        List<DeviceActionAvro> actions = new ArrayList<>();

        for (Scenario scenario : scenarios) {
            if (isScenarioFulfilled(scenario, snapshot)) {
                actions.addAll(scenario.getActions().stream()
                        .map(ScenarioAction::getAction)
                        .map(this::toAvroAction)
                        .toList());
            }
        }
        return actions;
    }

    private boolean isScenarioFulfilled(Scenario scenario, SensorsSnapshotAvro snapshot) {
        log.debug("Evaluating scenario: '{}' for hub: {}", scenario.getName(), scenario.getHubId());

        Map<String, SensorStateAvro> states = snapshot.getSensorsState();
        for (ScenarioCondition scenarioCondition : scenario.getConditions()) { // ← for-each вместо stream
            String sensorId = scenarioCondition.getSensorId();
            SensorStateAvro state = states.get(sensorId);

            if (state == null) {
                log.debug("Sensor {} not found in snapshot", sensorId);
                return false;
            }

            boolean conditionMet = checkCondition(scenarioCondition.getCondition(), state);
            log.debug("Condition for sensor {}: {} = {}", sensorId, scenarioCondition.getCondition().getType(), conditionMet);
            if (!conditionMet) {
                return false;
            }
        }
        return true;
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
            case "SWITCH" -> {
                if (data instanceof SwitchSensorAvro switchData) {
                    yield switchData.getState() ? 1 : 0;
                } else {
                    log.warn("Expected SwitchSensorAvro for type {}, got: {}", type, data.getClass());
                    yield 0;
                }
            }
            case "MOTION" -> {
                if (data instanceof MotionSensorAvro motionData) {
                    yield motionData.getMotion() ? 1 : 0;
                } else {
                    log.warn("Expected MotionSensorAvro for type {}, got: {}", type, data.getClass());
                    yield 0;
                }
            }
            case "LUMINOSITY" -> {
                if (data instanceof LightSensorAvro lightData) {
                    yield lightData.getLuminosity();
                } else {
                    log.warn("Expected LightSensorAvro for type {}, got: {}", type, data.getClass());
                    yield 0;
                }
            }
            case "TEMPERATURE", "HUMIDITY", "CO2LEVEL" -> {
                if (data instanceof ClimateSensorAvro climateData) {
                    yield switch (type) {
                        case "TEMPERATURE" -> climateData.getTemperatureC();
                        case "HUMIDITY" -> climateData.getHumidity();
                        case "CO2LEVEL" -> climateData.getCo2Level();
                        default -> 0;
                    };
                } else {
                    log.warn("Expected ClimateSensorAvro for type {}, got: {}", type, data.getClass());
                    yield 0;
                }
            }
            default -> {
                log.warn("Unknown sensor type: {}", type);
                yield 0;
            }
        };
    }

    private DeviceActionAvro toAvroAction(Action action) {
        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
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

    @Getter
    public static class ActionWithScenario {
        private final DeviceActionAvro action;
        private final String scenarioName;

        public ActionWithScenario(DeviceActionAvro action, String scenarioName) {
            this.action = action;
            this.scenarioName = scenarioName;
        }

    }

    public List<ActionWithScenario> evaluateWithScenarioNames(
            SensorsSnapshotAvro snapshot,
            List<Scenario> scenarios) {

        List<ActionWithScenario> actions = new ArrayList<>();

        for (Scenario scenario : scenarios) {
            if (isScenarioFulfilled(scenario, snapshot)) {
                for (var scenarioAction : scenario.getActions()) {
                    DeviceActionAvro avroAction = toAvroAction(scenarioAction.getAction());
                    actions.add(new ActionWithScenario(avroAction, scenario.getName()));
                }
            }
        }
        return actions;
    }
}