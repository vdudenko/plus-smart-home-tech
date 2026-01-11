package ru.yandex.practicum.collector.converter;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.ArrayList;
import java.util.List;

@Component
public class HubEventProtoConverter {

    public HubEventAvro toAvro(HubEventProto proto) {
        HubEventAvro.Builder builder = HubEventAvro.newBuilder()
                .setHubId(proto.getHubId())
                .setTimestamp(timestampToMillis(proto.getTimestamp()));

        switch (proto.getPayloadCase()) {
            case DEVICE_ADDED:
                DeviceAddedEventProto added = proto.getDeviceAdded();
                builder.setPayload(
                        DeviceAddedEventAvro.newBuilder()
                                .setId(added.getId())
                                .setType(toAvroDeviceType(added.getType()))
                                .build()
                );
                break;

            case SCENARIO_ADDED:
                ScenarioAddedEventProto scenario = proto.getScenarioAdded();
                builder.setPayload(
                        ScenarioAddedEventAvro.newBuilder()
                                .setName(scenario.getName())
                                .setConditions(toAvroConditions(scenario.getConditionList()))
                                .setActions(toAvroActions(scenario.getActionList()))
                                .build()
                );
                break;

            case DEVICE_REMOVED:
                DeviceRemovedEventProto removed = proto.getDeviceRemoved();
                builder.setPayload(
                        DeviceRemovedEventAvro.newBuilder()
                                .setId(removed.getId())
                                .build()
                );
                break;

            case SCENARIO_REMOVED:
                ScenarioRemovedEventProto removedScenario = proto.getScenarioRemoved();
                builder.setPayload(
                        ScenarioRemovedEventAvro.newBuilder()
                                .setName(removedScenario.getId())
                                .build()
                );
                break;

            default:
                throw new IllegalArgumentException("Unsupported hub event: " + proto.getPayloadCase());
        }

        return builder.build();
    }

    private long timestampToMillis(com.google.protobuf.Timestamp ts) {
        if (ts == null) return System.currentTimeMillis();
        return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
    }

    private DeviceTypeAvro toAvroDeviceType(DeviceTypeProto type) {
        return switch (type) {
            case MOTION_SENSOR -> DeviceTypeAvro.MOTION_SENSOR;
            case TEMPERATURE_SENSOR -> DeviceTypeAvro.TEMPERATURE_SENSOR;
            case LIGHT_SENSOR -> DeviceTypeAvro.LIGHT_SENSOR;
            case CLIMATE_SENSOR -> DeviceTypeAvro.CLIMATE_SENSOR;
            case SWITCH_SENSOR -> DeviceTypeAvro.SWITCH_SENSOR;
            default -> throw new IllegalArgumentException("Unknown device type: " + type);
        };
    }

    private ConditionTypeAvro toAvroConditionType(ConditionTypeProto type) {
        return switch (type) {
            case MOTION -> ConditionTypeAvro.MOTION;
            case LUMINOSITY -> ConditionTypeAvro.LUMINOSITY;
            case SWITCH -> ConditionTypeAvro.SWITCH;
            case TEMPERATURE -> ConditionTypeAvro.TEMPERATURE;
            case CO2LEVEL -> ConditionTypeAvro.CO2LEVEL;
            case HUMIDITY -> ConditionTypeAvro.HUMIDITY;
            default -> throw new IllegalArgumentException("Unknown condition type: " + type);
        };
    }

    private ConditionOperationAvro toAvroOperation(ConditionOperationProto op) {
        return switch (op) {
            case EQUALS -> ConditionOperationAvro.EQUALS;
            case GREATER_THAN -> ConditionOperationAvro.GREATER_THAN;
            case LOWER_THAN -> ConditionOperationAvro.LOWER_THAN;
            default -> throw new IllegalArgumentException("Unknown operation: " + op);
        };
    }

    private ActionTypeAvro toAvroActionType(ActionTypeProto type) {
        return switch (type) {
            case ACTIVATE -> ActionTypeAvro.ACTIVATE;
            case DEACTIVATE -> ActionTypeAvro.DEACTIVATE;
            case INVERSE -> ActionTypeAvro.INVERSE;
            case SET_VALUE -> ActionTypeAvro.SET_VALUE;
            default -> throw new IllegalArgumentException("Unknown action type: " + type);
        };
    }

    private List<ScenarioConditionAvro> toAvroConditions(List<ScenarioConditionProto> conditions) {
        if (conditions == null) return new ArrayList<>();
        List<ScenarioConditionAvro> result = new ArrayList<>();
        for (ScenarioConditionProto c : conditions) {
            ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                    .setSensorId(c.getSensorId())
                    .setType(toAvroConditionType(c.getType()))
                    .setOperation(toAvroOperation(c.getOperation()));

            if (c.hasBoolValue()) {
                builder.setValue(c.getBoolValue());
            } else if (c.hasIntValue()) {
                builder.setValue(c.getIntValue());
            }

            result.add(builder.build());
        }
        return result;
    }

    private List<DeviceActionAvro> toAvroActions(List<DeviceActionProto> actions) {
        if (actions == null) return new ArrayList<>();
        List<DeviceActionAvro> result = new ArrayList<>();
        for (DeviceActionProto a : actions) {
            DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                    .setSensorId(a.getSensorId())
                    .setType(toAvroActionType(a.getType()));

            if (a.hasValue()) {
                builder.setValue(a.getValue());
            }

            result.add(builder.build());
        }
        return result;
    }
}