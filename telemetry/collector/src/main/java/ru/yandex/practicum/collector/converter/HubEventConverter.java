package ru.yandex.practicum.collector.converter;

import ru.yandex.practicum.kafka.telemetry.event.*;
import telemetry.service.collector.*;

import java.util.ArrayList;
import java.util.List;

public class HubEventConverter {

    public static HubEventAvro toAvro(CollectHubEventRequest request) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(request.getHubId())
                .setTimestamp(request.getTimestamp());

        switch (request.getPayloadCase()) {
            case DEVICE_ADDED -> {
                DeviceAddedEventAvro avro = DeviceAddedEventAvro.newBuilder()
                        .setId(request.getDeviceAdded().getId())
                        .setType(toAvroDeviceType(request.getDeviceAdded().getDeviceType()))
                        .build();
                builder.setPayload(avro);
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEventAvro avro = DeviceRemovedEventAvro.newBuilder()
                        .setId(request.getDeviceRemoved().getId())
                        .build();
                builder.setPayload(avro);
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEventAvro avro = ScenarioAddedEventAvro.newBuilder()
                        .setName(request.getScenarioAdded().getName())
                        .setConditions(toAvroConditions(request.getScenarioAdded().getConditionsList()))
                        .setActions(toAvroActions(request.getScenarioAdded().getActionsList()))
                        .build();
                builder.setPayload(avro);
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEventAvro avro = ScenarioRemovedEventAvro.newBuilder()
                        .setName(request.getScenarioRemoved().getName())
                        .build();
                builder.setPayload(avro);
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Hub event payload is not set");
        }

        return builder.build();
    }

    private static DeviceTypeAvro toAvroDeviceState(DeviceType type) {
        return switch (type) {
            case DEVICE_TYPE_UNSPECIFIED -> DeviceTypeAvro.MOTION_SENSOR;
            case MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
            default -> throw new IllegalArgumentException("Unsupported DeviceType: " + type);
        };
    }

    private static ConditionTypeAvro toAvroConditionType(ConditionType type) {
        return switch (type) {
            case CONDITION_TYPE_UNSPECIFIED -> ConditionTypeAvro.MOTION;
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            default -> throw new IllegalArgumentException("Unsupported ConditionType: " + type);
        };
    }

    private static ConditionOperationAvro toAvroConditionOperation(ConditionOperation op) {
        return switch (op) {
            case CONDITION_OPERATION_UNSPECIFIED -> ConditionOperationAvro.EQUALS;
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            default -> throw new IllegalArgumentException("Unsupported ConditionOperation: " + op);
        };
    }

    private static ActionTypeAvro toAvroActionType(ActionType type) {
        return switch (type) {
            case ACTION_TYPE_UNSPECIFIED -> ActionTypeAvro.ACTIVATE;
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
            default -> throw new IllegalArgumentException("Unsupported ActionType: " + type);
        };
    }

    private static List<ScenarioConditionAvro> toAvroConditions(
            List<ScenarioCondition> conditions) {
        if (conditions == null) return new ArrayList<>();
        List<ScenarioConditionAvro> result = new ArrayList<>();
        for (ScenarioCondition c : conditions) {
            ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                    .setSensorId(c.getSensorId())
                    .setType(toAvroConditionType(c.getType()))
                    .setOperation(toAvroConditionOperation(c.getOperation()));

            switch (c.getValueCase()) {
                case INT_VALUE -> builder.setValue(c.getIntValue());
                case BOOL_VALUE -> builder.setValue(c.getBoolValue());
                case VALUE_NOT_SET -> builder.setValue((Void) null);
                default -> throw new IllegalArgumentException("Unknown value type in condition");
            }
            result.add(builder.build());
        }
        return result;
    }

    private static List<DeviceActionAvro> toAvroActions(
            List<DeviceAction> actions) {
        if (actions == null) return new ArrayList<>();
        List<DeviceActionAvro> result = new ArrayList<>();
        for (DeviceAction a : actions) {
            DeviceActionAvro avro = DeviceActionAvro.newBuilder()
                    .setSensorId(a.getSensorId())
                    .setType(toAvroActionType(a.getType()))
                    .setValue(a.getValue())
                    .build();
            result.add(avro);
        }
        return result;
    }

    private static DeviceTypeAvro toAvroDeviceType(DeviceType type) {
        return toAvroDeviceState(type);
    }
}
