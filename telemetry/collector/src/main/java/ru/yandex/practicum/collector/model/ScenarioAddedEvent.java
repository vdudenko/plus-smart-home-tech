package ru.yandex.practicum.collector.model;

import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.collector.type.HubEventType;

import java.util.List;

@Getter
@Setter
@JsonTypeName("SCENARIO_ADDED")
public class ScenarioAddedEvent extends HubEvent {
    private String name;
    private List<ScenarioCondition> conditions;
    private List<DeviceAction> actions;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }
}
