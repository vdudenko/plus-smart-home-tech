package ru.yandex.practicum.collector.converter;

import ru.yandex.practicum.collector.model.*;
import ru.yandex.practicum.collector.type.ActionType;
import ru.yandex.practicum.collector.type.ConditionOperation;
import ru.yandex.practicum.collector.type.ConditionType;
import ru.yandex.practicum.collector.type.DeviceType;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.ArrayList;
import java.util.List;

public class HubEventConverter {

    public static HubEventAvro toAvro(HubEvent event) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli());

        if (event instanceof DeviceAddedEvent deviceAdded) {
            DeviceAddedEventAvro payload = DeviceAddedEventAvro.newBuilder()
                    .setId(deviceAdded.getId())
                    .setType(toAvroDeviceType(deviceAdded.getDeviceType()))
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof DeviceRemovedEvent deviceRemoved) {
            DeviceRemovedEventAvro payload = DeviceRemovedEventAvro.newBuilder()
                    .setId(deviceRemoved.getId())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof ScenarioAddedEvent scenarioAdded) {
            ScenarioAddedEventAvro payload = ScenarioAddedEventAvro.newBuilder()
                    .setName(scenarioAdded.getHubId())
                    .setConditions(toAvroConditions(scenarioAdded.getConditions()))
                    .setActions(toAvroActions(scenarioAdded.getActions()))
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof ScenarioRemovedEvent scenarioRemoved) {
            ScenarioRemovedEventAvro payload = ScenarioRemovedEventAvro.newBuilder()
                    .setName(scenarioRemoved.getHubId())
                    .build();
            builder.setPayload(payload);
        } else {
            throw new IllegalArgumentException("Unknown HubEvent type: " + event.getClass());
        }

        return builder.build();
    }

    // --- Вспомогательные методы ---

    private static DeviceTypeAvro toAvroDeviceType(DeviceType type) {
        return switch (type) {
            case MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
        };
    }

    private static ConditionTypeAvro toAvroConditionType(ConditionType type) {
        return switch (type) {
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
        };
    }

    private static ConditionOperationAvro toAvroConditionOperation(ConditionOperation op) {
        return switch (op) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
        };
    }

    private static ActionTypeAvro toAvroActionType(ActionType type) {
        return switch (type) {
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
        };
    }

    private static List<ScenarioConditionAvro> toAvroConditions(List<ScenarioCondition> conditions) {
        if (conditions == null) return new ArrayList<>();
        List<ScenarioConditionAvro> result = new ArrayList<>();
        for (ScenarioCondition c : conditions) {
            ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                    .setSensorId(c.getSensorId())
                    .setType(toAvroConditionType(c.getType()))
                    .setOperation(toAvroConditionOperation(c.getOperation()));

            // Обработка value: может быть Integer или Boolean
            Object value = c.getValue();
            if (value instanceof Integer) {
                builder.setValue((Integer) value);
            } else if (value instanceof Boolean) {
                builder.setValue((Boolean) value);
            } // иначе null — оставляем по умолчанию

            result.add(builder.build());
        }
        return result;
    }

    private static List<DeviceActionAvro> toAvroActions(List<DeviceAction> actions) {
        if (actions == null) return new ArrayList<>();
        List<DeviceActionAvro> result = new ArrayList<>();
        for (DeviceAction a : actions) {
            DeviceActionAvro avro = DeviceActionAvro.newBuilder()
                    .setSensorId(a.getSensorId())
                    .setType(toAvroActionType(a.getType()))
                    .setValue(a.getValue() != null ? a.getValue() : 0)
                    .build();
            result.add(avro);
        }
        return result;
    }
}