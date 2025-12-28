package ru.yandex.practicum.collector.model;

import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.collector.type.DeviceType;
import ru.yandex.practicum.collector.type.HubEventType;
import com.fasterxml.jackson.annotation.JsonTypeName;

@Getter
@Setter
@JsonTypeName("DEVICE_ADDED")
public class DeviceAddedEvent extends HubEvent {
    private String id;
    private DeviceType deviceType;

    @Override
    public HubEventType getType() {
        return HubEventType.DEVICE_ADDED;
    }
}
