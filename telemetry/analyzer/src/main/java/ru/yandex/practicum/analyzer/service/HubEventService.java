package ru.yandex.practicum.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.entity.*;
import ru.yandex.practicum.analyzer.repository.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubEventService {

    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Transactional
    public void process(HubEventAvro event) {
        switch (event.getPayload().getClass().getSimpleName()) {
            case "DeviceAddedEventAvro" -> handleDeviceAdded(event);
            case "ScenarioAddedEventAvro" -> handleScenarioAdded(event);
            default -> log.warn("Unsupported event type: {}", event.getPayload().getClass());
        }
    }

    private void handleDeviceAdded(HubEventAvro event) {
        DeviceAddedEventAvro payload = (DeviceAddedEventAvro) event.getPayload();
        Sensor sensor = new Sensor();
        sensor.setId(payload.getId());
        sensor.setHubId(event.getHubId());
        sensorRepository.save(sensor);
        log.info("Saved device: {} for hub: {}", payload.getId(), event.getHubId());
    }

    private void handleScenarioAdded(HubEventAvro event) {
        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) event.getPayload();

        List<Condition> conditions = new ArrayList<>();
        for (var cond : payload.getConditions()) {
            Condition condition = new Condition();
            condition.setType(cond.getType().name());
            condition.setOperation(cond.getOperation().name());
            condition.setValue(cond.getValue() instanceof Boolean ?
                    ((Boolean) cond.getValue() ? 1 : 0) :
                    (Integer) cond.getValue());
            conditions.add(conditionRepository.save(condition));
        }

        List<Action> actions = new ArrayList<>();
        for (var act : payload.getActions()) {
            Action action = new Action();
            action.setType(act.getType().name());
            action.setValue(act.getValue());
            actions.add(actionRepository.save(action));
        }

        Scenario scenario = new Scenario();
        scenario.setHubId(event.getHubId());
        scenario.setName(payload.getName());
        scenario.setConditions(conditions);
        scenario.setActions(actions);
        scenarioRepository.save(scenario);

        log.info("Saved scenario: {} for hub: {}", payload.getName(), event.getHubId());
    }
}