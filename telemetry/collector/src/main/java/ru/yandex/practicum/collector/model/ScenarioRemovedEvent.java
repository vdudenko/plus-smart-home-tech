package ru.yandex.practicum.collector.model;

import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.collector.type.HubEventType;

@Getter
@Setter
public class ScenarioRemovedEvent extends HubEvent {
    private String name;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_REMOVED;
    }
}
