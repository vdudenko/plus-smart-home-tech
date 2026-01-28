package ru.yandex.practicum.collector.converter;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

@Component
public class SensorEventProtoConverter {

    public SensorEventAvro toAvro(SensorEventProto proto) {
        if (proto == null) {
            throw new IllegalArgumentException("SensorEventProto cannot be null");
        }

        SensorEventAvro.Builder builder = SensorEventAvro.newBuilder()
                .setId(proto.getId())
                .setHubId(proto.getHubId())
                .setTimestamp(timestampToMillis(proto.getTimestamp()));

        switch (proto.getPayloadCase()) {
            case CLIMATE_SENSOR:
                var climate = proto.getClimateSensor();
                builder.setPayload(ClimateSensorAvro.newBuilder()
                        .setTemperatureC(climate.getTemperatureC())
                        .setHumidity(climate.getHumidity())
                        .setCo2Level(climate.getCo2Level())
                        .build());
                break;

            case LIGHT_SENSOR:
                var light = proto.getLightSensor();
                builder.setPayload(LightSensorAvro.newBuilder()
                        .setLinkQuality(light.getLinkQuality())
                        .setLuminosity(light.getLuminosity())
                        .build());
                break;

            case MOTION_SENSOR:
                var motion = proto.getMotionSensor();
                builder.setPayload(MotionSensorAvro.newBuilder()
                        .setLinkQuality(motion.getLinkQuality())
                        .setMotion(motion.getMotion())
                        .setVoltage(motion.getVoltage())
                        .build());
                break;

            case SWITCH_SENSOR:
                var switchSensor = proto.getSwitchSensor();
                builder.setPayload(SwitchSensorAvro.newBuilder()
                        .setState(switchSensor.getState())
                        .build());
                break;

            case TEMPERATURE_SENSOR:
                var temperature = proto.getTemperatureSensor();
                builder.setPayload(TemperatureSensorAvro.newBuilder()
                        .setTemperatureC(temperature.getTemperatureC())
                        .setTemperatureF(temperature.getTemperatureF())
                        .build());
                break;

            default:
                throw new IllegalArgumentException("Unsupported sensor event type: " + proto.getPayloadCase());
        }

        return builder.build();
    }

    private static long timestampToMillis(com.google.protobuf.Timestamp ts) {
        if (ts == null) {
            return System.currentTimeMillis();
        }
        return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
    }
}