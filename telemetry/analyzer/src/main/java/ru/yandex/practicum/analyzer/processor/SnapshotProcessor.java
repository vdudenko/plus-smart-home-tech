package ru.yandex.practicum.analyzer.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.controller.HubRouterClient;
import ru.yandex.practicum.analyzer.deserializer.HubEventAvroDeserializer;
import ru.yandex.practicum.analyzer.deserializer.SensorsSnapshotAvroDeserializer;
import ru.yandex.practicum.analyzer.service.HubEventService;
import ru.yandex.practicum.analyzer.service.ScenarioEvaluator;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final HubEventService hubEventService;
    private final ScenarioEvaluator scenarioEvaluator;
    private final HubRouterClient hubRouterClient;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private KafkaConsumer<String, HubEventAvro> hubConsumer;
    private KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;

    public void start() {
        // Инициализация потребителя для событий хаба
        Properties hubProps = new Properties();
        hubProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        hubProps.put(ConsumerConfig.GROUP_ID_CONFIG, "analyzer-single-group");
        hubProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        hubProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HubEventAvroDeserializer.class);
        hubProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        hubProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        hubConsumer = new KafkaConsumer<>(hubProps);
        hubConsumer.subscribe(Collections.singletonList("telemetry.hubs.v1"));

        // Инициализация потребителя для снапшотов
        Properties snapshotProps = new Properties();
        snapshotProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        snapshotProps.put(ConsumerConfig.GROUP_ID_CONFIG, "analyzer-single-group");
        snapshotProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        snapshotProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorsSnapshotAvroDeserializer.class);
        snapshotProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        snapshotProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        snapshotConsumer = new KafkaConsumer<>(snapshotProps);
        snapshotConsumer.subscribe(Collections.singletonList("telemetry.snapshots.v1"));

        log.info("SnapshotProcessor started");

        // Запуск потока для обработки событий хаба
        Thread hubThread = new Thread(this::processHubEvents, "HubEventThread");
        hubThread.start();

        // Основной поток — обработка снапшотов
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = snapshotConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    processSnapshot(record.value());
                }
                snapshotConsumer.commitSync();
            }
        } catch (WakeupException e) {
            log.info("SnapshotProcessor woken up for shutdown");
        } catch (Exception e) {
            log.error("Error in SnapshotProcessor", e);
        } finally {
            snapshotConsumer.close();
            hubConsumer.wakeup();
            try {
                hubThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void processHubEvents() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, HubEventAvro> records = hubConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    hubEventService.process(record.value());
                }
                hubConsumer.commitSync();
            }
        } catch (WakeupException e) {
            log.info("HubEventProcessor woken up");
        } catch (Exception e) {
            log.error("Error in HubEventProcessor", e);
        } finally {
            hubConsumer.close();
        }
    }

    private void processSnapshot(SensorsSnapshotAvro snapshot) {
        try {
            String hubId = snapshot.getHubId();
            log.debug("Processing snapshot for hub: {}", hubId);

            var scenarios = hubEventService.findByHubIdWithConditionsAndActions(hubId);
            if (scenarios.isEmpty()) {
                log.debug("No scenarios found for hub: {}", hubId);
                return;
            }

            // Оцениваем сценарии и получаем действия
            List<DeviceActionAvro> actions = scenarioEvaluator.evaluate(snapshot, scenarios);
            if (actions.isEmpty()) {
                log.debug("No conditions met for hub: {}", hubId);
                return;
            }

            // Отправляем команды в Hub Router
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