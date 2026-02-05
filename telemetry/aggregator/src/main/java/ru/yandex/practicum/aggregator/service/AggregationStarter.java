package ru.yandex.practicum.aggregator.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.deserializer.SensorEventAvroDeserializer;
import ru.yandex.practicum.aggregator.util.SnapshotManager;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final SnapshotManager snapshotManager;
    private KafkaConsumer<String, SensorEventAvro> consumer;
    private KafkaProducer<String, byte[]> producer;

    @PostConstruct
    public void init() {
        snapshotManager.clear();
        log.info("SnapshotManager cleared");

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "aggregator-group-" + System.currentTimeMillis()); // уникальная группа для каждого запуска
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventAvroDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // ← КРИТИЧЕСКИ ВАЖНО: читаем ТОЛЬКО новые события
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(consumerProps);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProps);
    }

    public void start() {
        consumer.subscribe(Collections.singletonList("telemetry.sensors.v1"));
        log.info("Aggregator started");

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
            // нормальное завершение
        } finally {
            shutdownResources();
        }
    }

    private void processSensorEvent(SensorEventAvro event) {
        try {
            snapshotManager.updateState(event).ifPresent(snapshot -> {
                byte[] data = serializeAvro(snapshot);
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                        "telemetry.snapshots.v1",
                        snapshot.getHubId(),
                        data
                );
                try {
                    producer.send(record).get(1, TimeUnit.SECONDS);
                    log.info("SNAPSHOT_PUBLISHED hub={} devices={}",
                            snapshot.getHubId(), snapshot.getSensorsState().size());
                } catch (Exception e) {
                    log.error("Failed to publish snapshot", e);
                }
            });
        } catch (Exception e) {
            log.error("Error processing event", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (consumer != null) consumer.wakeup();
    }

    private void shutdownResources() {
        try {
            if (producer != null) producer.close();
            if (consumer != null) consumer.close();
        } catch (Exception e) {
            log.warn("Error on shutdown", e);
        }
    }

    private byte[] serializeAvro(org.apache.avro.specific.SpecificRecord avro) {
        try (java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {
            org.apache.avro.io.DatumWriter<org.apache.avro.specific.SpecificRecord> writer =
                    new org.apache.avro.specific.SpecificDatumWriter<>(avro.getSchema());
            org.apache.avro.io.BinaryEncoder encoder =
                    org.apache.avro.io.EncoderFactory.get().binaryEncoder(out, null);
            writer.write(avro, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }
}