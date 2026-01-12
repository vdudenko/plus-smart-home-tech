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
    private final DeferredScenarioQueue deferredScenarioQueue;

    @Transactional
    public void process(HubEventAvro event) {
        switch (event.getPayload().getClass().getSimpleName()) {
            case "DeviceAddedEventAvro" -> handleDeviceAdded(event);
            case "ScenarioAddedEventAvro" -> handleScenarioAdded(event);
            default -> log.warn("Unsupported hub event type: {}", event.getPayload().getClass().getSimpleName());
        }
    }

    private void handleDeviceAdded(HubEventAvro event) {
        DeviceAddedEventAvro payload = (DeviceAddedEventAvro) event.getPayload();
        Sensor sensor = new Sensor();
        sensor.setId(payload.getId());
        sensor.setHubId(event.getHubId());
        sensorRepository.save(sensor);
        log.info("Registered device: {} for hub: {}", payload.getId(), event.getHubId());

        // Пытаемся обработать отложенные сценарии
        processDeferredScenarios();
    }

    private void handleScenarioAdded(HubEventAvro event) {
        ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) event.getPayload();

        // Проверяем, существует ли уже такой сценарий
        if (scenarioRepository.findByHubIdAndName(event.getHubId(), payload.getName()).isPresent()) {
            log.debug("Scenario '{}' for hub '{}' already exists, skipping",
                    payload.getName(), event.getHubId());
            return;
        }

        // Проверяем, что все датчики из сценария уже зарегистрированы
        if (!areAllSensorsRegistered(payload)) {
            log.warn("Not all sensors registered for scenario '{}', deferring...", payload.getName());
            deferredScenarioQueue.add(event);
            return;
        }

        saveScenario(event, payload);
        log.info("Saved scenario: '{}' for hub: {}", payload.getName(), event.getHubId());
    }

    private boolean areAllSensorsRegistered(ScenarioAddedEventAvro payload) {
        // Проверяем датчики из условий
        for (var condition : payload.getConditions()) {
            if (!sensorRepository.existsById(condition.getSensorId())) {
                return false;
            }
        }
        // Проверяем датчики из действий
        for (var action : payload.getActions()) {
            if (!sensorRepository.existsById(action.getSensorId())) {
                return false;
            }
        }
        return true;
    }

    private void saveScenario(HubEventAvro event, ScenarioAddedEventAvro payload) {
        Scenario scenario = new Scenario();
        scenario.setHubId(event.getHubId());
        scenario.setName(payload.getName());

        // Сохраняем условия
        for (var cond : payload.getConditions()) {
            Condition condition = new Condition();
            condition.setType(cond.getType().name());
            condition.setOperation(cond.getOperation().name());
            condition.setValue(cond.getValue() instanceof Boolean ?
                    ((Boolean) cond.getValue() ? 1 : 0) :
                    (Integer) cond.getValue());
            condition = conditionRepository.save(condition);

            ScenarioCondition sc = new ScenarioCondition();
            sc.setScenario(scenario);
            sc.setSensorId(cond.getSensorId());
            sc.setCondition(condition);
            scenario.getConditions().add(sc);
        }

        // Сохраняем действия
        for (var act : payload.getActions()) {
            Action action = new Action();
            action.setSensorId(act.getSensorId());
            action.setType(act.getType().name());
            action.setValue(act.getValue());
            action = actionRepository.save(action);

            ScenarioAction sa = new ScenarioAction();
            sa.setScenario(scenario);
            sa.setSensorId(act.getSensorId());
            sa.setAction(action);
            scenario.getActions().add(sa);
        }

        scenarioRepository.save(scenario);
    }

    private void processDeferredScenarios() {
        deferredScenarioQueue.cleanupOlderThan(300_000);

        List<DeferredScenario> toProcess = new ArrayList<>(deferredScenarioQueue.getAll());
        for (DeferredScenario deferred : toProcess) {
            ScenarioAddedEventAvro payload = (ScenarioAddedEventAvro) deferred.getEvent().getPayload();

            // Проверка на дубликат
            if (scenarioRepository.findByHubIdAndName(
                    deferred.getEvent().getHubId(), payload.getName()).isPresent()) {
                deferredScenarioQueue.remove(deferred);
                continue;
            }

            if (areAllSensorsRegistered(payload)) {
                saveScenario(deferred.getEvent(), payload);
                deferredScenarioQueue.remove(deferred);
                log.info("Processed deferred scenario: '{}' for hub: {}",
                        payload.getName(), deferred.getEvent().getHubId());
            }
        }
    }
}