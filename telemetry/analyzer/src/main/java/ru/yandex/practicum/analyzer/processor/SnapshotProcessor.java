package ru.yandex.practicum.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.controller.HubRouterClient;
import ru.yandex.practicum.analyzer.deserializer.SensorsSnapshotAvroDeserializer;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.service.ScenarioEvaluator;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final ScenarioRepository scenarioRepository;
    private final ScenarioEvaluator scenarioEvaluator;
    private final HubRouterClient hubRouterClient;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "analyzer-snapshots");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, SensorsSnapshotAvro> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("telemetry.snapshots.v1"));
            log.info("SnapshotProcessor started. Listening to telemetry.snapshots.v1...");

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    processSnapshot(record.value());
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            log.info("SnapshotProcessor woken up for shutdown");
        } catch (Exception e) {
            log.error("Error in SnapshotProcessor", e);
        }
    }

    private void processSnapshot(SensorsSnapshotAvro snapshot) {
        try {
            String hubId = snapshot.getHubId();
            log.debug("Processing snapshot for hub: {}", hubId);

            var scenarios = scenarioRepository.findByHubId(hubId);
            if (scenarios.isEmpty()) {
                log.debug("No scenarios found for hub: {}", hubId);
                return;
            }

            List<DeviceActionAvro> actions = scenarioEvaluator.evaluate(snapshot, scenarios);
            if (actions.isEmpty()) {
                log.debug("No conditions met for hub: {}", hubId);
                return;
            }

            long timestamp = System.currentTimeMillis();
            for (DeviceActionAvro action : actions) {
                hubRouterClient.sendAction(hubId, "triggered-scenario", action, timestamp);
            }

            log.info("Executed {} actions for hub: {}", actions.size(), hubId);
        } catch (Exception e) {
            log.error("Failed to process snapshot for hub: {}", snapshot.getHubId(), e);
        }
    }
}