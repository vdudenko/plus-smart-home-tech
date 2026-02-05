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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AggregationStarter {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private KafkaConsumer<String, SensorEventAvro> consumer;
    private KafkaProducer<String, byte[]> producer;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private final Map<String, Map<String, Map<String, String>>> snapshots = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("🔧 Initializing Aggregator with Kafka at {}", bootstrapServers);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "aggregator-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventAvroDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(consumerProps);
        log.info("Kafka Consumer initialized for topic telemetry.sensors.v1");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProps);
        log.info("Kafka Producer initialized for topic telemetry.snapshots.v1");
    }

    public void start() {
        consumer.subscribe(Collections.singletonList("telemetry.sensors.v1"));
        log.info("Aggregator STARTED. Listening to telemetry.sensors.v1...");
        log.info("First snapshot will be published AFTER first sensor event is processed");

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
            log.error("FATAL ERROR in aggregator", e);
        } finally {
            shutdownResources();
        }
    }

    private void processSensorEvent(SensorEventAvro event) {
        try {
            String hubId = event.getHubId();
            String deviceId = event.getId();
            Object payload = event.getPayload();

            log.debug("Processing event: hubId={}, deviceId={}, payloadType={}",
                    hubId, deviceId, payload != null ? payload.getClass().getSimpleName() : "null");

            // Получаем/создаём снапшот для хаба
            Map<String, Map<String, String>> hubSnapshot = snapshots.computeIfAbsent(hubId, k -> new ConcurrentHashMap<>());
            Map<String, String> deviceState = hubSnapshot.computeIfAbsent(deviceId, k -> new ConcurrentHashMap<>());
            Map<String, String> previousState = new HashMap<>(deviceState); // Копия ДО изменений

            // Обрабатываем payload в зависимости от типа
            if (payload instanceof ClimateSensorAvro) {
                ClimateSensorAvro climate = (ClimateSensorAvro) payload;
                deviceState.put("temperature", String.valueOf(climate.getTemperatureC()));
                deviceState.put("humidity", String.valueOf(climate.getHumidity()));
                log.debug("Climate sensor: temp={}, humidity={}", climate.getTemperatureC(), climate.getHumidity());
            }
            else if (payload instanceof LightSensorAvro) {
                LightSensorAvro light = (LightSensorAvro) payload;
                deviceState.put("illumination", String.valueOf(light.getLuminosity()));
                log.debug("Light sensor: illumination={}", light.getLuminosity());
            }
            else if (payload instanceof MotionSensorAvro) {
                MotionSensorAvro motion = (MotionSensorAvro) payload;
                deviceState.put("motion", String.valueOf(motion.getMotion()));
                log.debug("Motion sensor: motion={}", motion.getMotion());
            }
            else if (payload instanceof SwitchSensorAvro) {
                SwitchSensorAvro sw = (SwitchSensorAvro) payload;
                deviceState.put("state", String.valueOf(sw.getState()));
                log.debug("Switch sensor: state={}", sw.getState());
            }
            else if (payload instanceof TemperatureSensorAvro) {
                TemperatureSensorAvro temp = (TemperatureSensorAvro) payload;
                deviceState.put("temperature", String.valueOf(temp.getTemperatureC()));
                log.debug("Temperature sensor: temp={}", temp.getTemperatureC());
            }
            else {
                log.warn("Unknown payload type: {}", payload != null ? payload.getClass().getSimpleName() : "null");
                return;
            }

            if (previousState.equals(deviceState)) {
                log.debug("State unchanged for device {}@{}, skipping publication", deviceId, hubId);
                return;
            }

            byte[] snapshotBytes;
            try {
                snapshotBytes = objectMapper.writeValueAsBytes(hubSnapshot);
            } catch (JsonProcessingException e) {
                log.error("Serialization error for hub {}", hubId, e);
                return;
            }

            // Публикуем снапшот в топик (СИНХРОННО для гарантии в тестах)
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    "telemetry.snapshots.v1",
                    hubId,
                    snapshotBytes
            );

            try {
                RecordMetadata metadata = producer.send(record).get(); // Синхронная отправка
                log.info("PUBLISHED SNAPSHOT for hub {} | partition={}, offset={} | devices={}",
                        hubId, metadata.partition(), metadata.offset(), hubSnapshot.size());
                log.debug("📊 Snapshot content: {}", new String(snapshotBytes, StandardCharsets.UTF_8));
            } catch (Exception e) {
                log.error("Failed to publish snapshot for hub {}", hubId, e);
            }

            producer.flush(); // Дополнительная гарантия для тестов

        } catch (Exception e) {
            log.error("Error processing event: hubId={}, deviceId={}",
                    event.getHubId(), event.getId(), e);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down aggregator...");
        if (consumer != null) consumer.wakeup();
    }

    private void shutdownResources() {
        try {
            if (producer != null) producer.flush();
            if (consumer != null) consumer.commitSync();
        } catch (Exception e) {
            log.warn("Error during shutdown", e);
        } finally {
            if (consumer != null) consumer.close();
            if (producer != null) producer.close();
            log.info("Aggregator STOPPED");
        }
    }
}