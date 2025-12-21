package ru.yandex.practicum.collector.model;

import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.collector.type.SensorEventType;

@Getter
@Setter
public class SwitchSensorEvent extends SensorEvent {
    private boolean state;

    @Override
    public SensorEventType getType() {
        return SensorEventType.SWITCH_SENSOR_EVENT;
    }
}