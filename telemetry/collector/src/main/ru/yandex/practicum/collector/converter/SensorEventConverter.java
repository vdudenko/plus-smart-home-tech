package ru.yandex.practicum.collector.converter;

import ru.yandex.practicum.collector.model.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;

public class SensorEventConverter {

    public static SensorEventAvro toAvro(SensorEvent event) {
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli());

        if (event instanceof ClimateSensorEvent e) {
            builder.setPayload(ClimateSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setHumidity(e.getHumidity())
                    .setCo2Level(e.getCo2Level())
                    .build());
        } else if (event instanceof LightSensorEvent e) {
            builder.setPayload(LightSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setLuminosity(e.getLuminosity())
                    .build());
        } else if (event instanceof MotionSensorEvent e) {
            builder.setPayload(MotionSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setMotion(e.isMotion())
                    .setVoltage(e.getVoltage())
                    .build());
        } else if (event instanceof SwitchSensorEvent e) {
            builder.setPayload(SwitchSensorAvro.newBuilder()
                    .setState(e.isState())
                    .build());
        } else if (event instanceof TemperatureSensorEvent e) {
            builder.setPayload(TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setTemperatureF(e.getTemperatureF())
                    .build());
        }

        return builder.build();
    }
}
