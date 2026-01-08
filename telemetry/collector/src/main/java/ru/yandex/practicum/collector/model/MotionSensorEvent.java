package ru.yandex.practicum.collector.model;

import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.collector.type.SensorEventType;

@Getter
@Setter
public class MotionSensorEvent extends SensorEvent {
    private int linkQuality;
    private boolean motion;
    private int voltage;

    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_EVENT;
    }
}
