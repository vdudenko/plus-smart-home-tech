package ru.yandex.practicum.aggregator.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.deserializer.SensorEventAvroDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AggregationStarter {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private KafkaConsumer<String, SensorEventAvro> consumer;
    private KafkaProducer<String, byte[]> producer;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final Map<String, Map<String, Map<String, String>>> snapshots = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "aggregator-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventAvroDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(consumerProps);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProps);
    }

    public void start() {
        consumer.subscribe(Collections.singletonList("telemetry.sensors.v1"));
        log.info("✅ Aggregator started. Listening to telemetry.sensors.v1...");

        try {
            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    processSensorEvent(record.value());
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer woken up for shutdown");
        } catch (Exception e) {
            log.error("❌ Fatal error in aggregator", e);
        } finally {
            shutdownResources();
        }
    }

    private void processSensorEvent(SensorEventAvro event) {
        String hubId = event.getHubId();
        String deviceId = event.getId();

        log.debug("📥 Processing event: hubId={}, deviceId={}", hubId, deviceId);

        Map<String, Map<String, String>> hubSnapshot = snapshots.computeIfAbsent(hubId, k -> new ConcurrentHashMap<>());
        Map<String, String> deviceState = hubSnapshot.computeIfAbsent(deviceId, k -> new ConcurrentHashMap<>());
        Map<String, String> previousState = new HashMap<>(deviceState);

        // 🔑 КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: обработка union-типа через проверку типа объекта
        Object payload = event.getPayload();

        if (payload instanceof ClimateSensorAvro) {
            ClimateSensorAvro climate = (ClimateSensorAvro) payload;
            deviceState.put("temperature", String.valueOf(climate.getTemperatureC()));
            deviceState.put("humidity", String.valueOf(climate.getHumidity()));
        }
        else if (payload instanceof LightSensorAvro) {
            LightSensorAvro light = (LightSensorAvro) payload;
            deviceState.put("illumination", String.valueOf(light.getLuminosity()));
        }
        else if (payload instanceof MotionSensorAvro) {
            MotionSensorAvro motion = (MotionSensorAvro) payload;
            deviceState.put("motion", String.valueOf(motion.getMotion()));
        }
        else if (payload instanceof SwitchSensorAvro) {
            SwitchSensorAvro sw = (SwitchSensorAvro) payload;
            deviceState.put("state", String.valueOf(sw.getState()));
        }
        else if (payload instanceof TemperatureSensorAvro) {
            TemperatureSensorAvro temp = (TemperatureSensorAvro) payload;
            deviceState.put("temperature", String.valueOf(temp.getTemperatureC()));
        }
        else {
            log.warn("⚠️ Unknown payload type: {}", payload != null ? payload.getClass().getSimpleName() : "null");
            return;
        }

        if (previousState.equals(deviceState)) {
            log.debug("⏭️ State unchanged for device {}@{}, skipping publication", deviceId, hubId);
            return;
        }

        try {
            byte[] snapshotBytes = objectMapper.writeValueAsBytes(hubSnapshot);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("telemetry.snapshots.v1", hubId, snapshotBytes);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("✅ Published snapshot for hub {} (offset={})", hubId, metadata.offset());
                } else {
                    log.error("❌ Failed to publish snapshot for hub {}", hubId, exception);
                }
            });
            producer.flush(); // Гарантия отправки для тестов
        } catch (JsonProcessingException e) {
            log.error("❌ Serialization error for hub {}", hubId, e);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("🛑 Shutting down aggregator...");
        consumer.wakeup();
    }

    private void shutdownResources() {
        try {
            producer.flush();
            consumer.commitSync();
        } catch (Exception e) {
            log.warn("⚠️ Error during shutdown", e);
        } finally {
            consumer.close();
            producer.close();
            log.info("✅ Aggregator stopped");
        }
    }
}