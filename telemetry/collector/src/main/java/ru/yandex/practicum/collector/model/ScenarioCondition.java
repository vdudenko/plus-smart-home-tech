package ru.yandex.practicum.collector.model;

import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.collector.type.ConditionOperation;
import ru.yandex.practicum.collector.type.ConditionType;

@Getter
@Setter
public class ScenarioCondition {
    private String sensorId;
    private ConditionType type;
    private ConditionOperation operation;
    private Object value;
}