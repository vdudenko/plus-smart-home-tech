package ru.yandex.practicum.analyzer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Component
public class DeferredScenarioQueue {
    private final ConcurrentLinkedQueue<DeferredScenario> queue = new ConcurrentLinkedQueue<>();

    public void add(HubEventAvro event) {
        queue.add(new DeferredScenario(event, System.currentTimeMillis()));
        log.debug("Deferred scenario: {}", event.getHubId());
    }

    public List<DeferredScenario> getAll() {
        return new ArrayList<>(queue);
    }

    public void remove(DeferredScenario scenario) {
        queue.remove(scenario);
        log.debug("Processed deferred scenario: {}", scenario.getEvent().getHubId());
    }

    public void cleanupOlderThan(long maxAgeMs) {
        long now = System.currentTimeMillis();
        Iterator<DeferredScenario> it = queue.iterator();
        while (it.hasNext()) {
            DeferredScenario scenario = it.next();
            if (now - scenario.getTimestamp() > maxAgeMs) {
                it.remove();
                log.warn("Removed expired deferred scenario: {}", scenario.getEvent().getHubId());
            }
        }
    }
}