package ru.yandex.practicum.collector.converter;

import ru.yandex.practicum.kafka.telemetry.event.*;
import smart_home.collector.v1.*;

public class SensorEventConverter {

    public static SensorEventAvro toAvro(CollectSensorEventRequest request) {
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(request.getId())
                .setHubId(request.getHubId())
                .setTimestamp(request.getTimestamp());

        switch (request.getPayloadCase()) {
            case CLIMATE -> {
                ClimateSensorAvro avro = ClimateSensorAvro.newBuilder()
                        .setTemperatureC(request.getClimate().getTemperatureC())
                        .setHumidity(request.getClimate().getHumidity())
                        .setCo2Level(request.getClimate().getCo2Level())
                        .build();
                builder.setPayload(avro);
            }
            case LIGHT -> {
                LightSensorAvro avro = LightSensorAvro.newBuilder()
                        .setLinkQuality(request.getLight().getLinkQuality())
                        .setLuminosity(request.getLight().getLuminosity())
                        .build();
                builder.setPayload(avro);
            }
            case MOTION -> {
                MotionSensorAvro avro = MotionSensorAvro.newBuilder()
                        .setLinkQuality(request.getMotion().getLinkQuality())
                        .setMotion(request.getMotion().getMotion())
                        .setVoltage(request.getMotion().getVoltage())
                        .build();
                builder.setPayload(avro);
            }
            case SWITCH_SENSOR -> {
                SwitchSensorAvro avro = SwitchSensorAvro.newBuilder()
                        .setState(request.getSwitchSensor().getState())
                        .build();
                builder.setPayload(avro);
            }
            case TEMPERATURE -> {
                TemperatureSensorAvro avro = TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(request.getTemperature().getTemperatureC())
                        .setTemperatureF(request.getTemperature().getTemperatureF())
                        .build();
                builder.setPayload(avro);
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Payload is not set");
        }

        return builder.build();
    }
}
