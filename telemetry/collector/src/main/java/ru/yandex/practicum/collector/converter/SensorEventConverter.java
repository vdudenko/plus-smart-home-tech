package ru.yandex.practicum.collector.converter;

import ru.yandex.practicum.collector.model.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class SensorEventConverter {

    public static SensorEventAvro toAvro(SensorEvent event) {
        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp().toEpochMilli());

        if (event instanceof ClimateSensorEvent climate) {
            ClimateSensorAvro payload = ClimateSensorAvro.newBuilder()
                    .setTemperatureC(climate.getTemperatureC())
                    .setHumidity(climate.getHumidity())
                    .setCo2Level(climate.getCo2Level())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof LightSensorEvent light) {
            LightSensorAvro payload = LightSensorAvro.newBuilder()
                    .setLinkQuality(light.getLinkQuality())
                    .setLuminosity(light.getLuminosity())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof MotionSensorEvent motion) {
            MotionSensorAvro payload = MotionSensorAvro.newBuilder()
                    .setLinkQuality(motion.getLinkQuality())
                    .setMotion(motion.isMotion())
                    .setVoltage(motion.getVoltage())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof SwitchSensorEvent switchSensor) {
            SwitchSensorAvro payload = SwitchSensorAvro.newBuilder()
                    .setState(switchSensor.isState())
                    .build();
            builder.setPayload(payload);
        } else if (event instanceof TemperatureSensorEvent temperature) {
            TemperatureSensorAvro payload = TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(temperature.getTemperatureC())
                    .setTemperatureF(temperature.getTemperatureF())
                    .build();
            builder.setPayload(payload);
        } else {
            throw new IllegalArgumentException("Unknown SensorEvent type: " + event.getClass());
        }

        return builder.build();
    }
}