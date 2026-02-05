package ru.yandex.practicum.aggregator.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.deserializer.SensorEventAvroDeserializer;
import ru.yandex.practicum.aggregator.util.SnapshotManager;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final SnapshotManager snapshotManager;
    private KafkaConsumer<String, SensorEventAvro> consumer;
    private KafkaProducer<String, byte[]> producer;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

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
        log.info("Kafka Consumer initialized for topic telemetry.sensors.v1");

        // Producer для публикации снапшотов
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
            snapshotManager.updateState(event)
                    .ifPresent(snapshot -> {
                        byte[] snapshotBytes = serializeAvro(snapshot);

                        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                                "telemetry.snapshots.v1",
                                snapshot.getHubId(),
                                snapshotBytes
                        );

                        producer.send(record, (metadata, exception) -> {
                            if (exception == null) {
                                log.info("PUBLISHED SNAPSHOT for hub {} | partition={}, offset={}",
                                        snapshot.getHubId(), metadata.partition(), metadata.offset());
                            } else {
                                log.error("Failed to publish snapshot for hub {}", snapshot.getHubId(), exception);
                            }
                        });

                        // Гарантируем отправку для тестов (без блокировки основного потока)
                        producer.flush();
                    });
        } catch (Exception e) {
            log.error("💥 Error processing event: hubId={}, deviceId={}",
                    event.getHubId(), event.getId(), e);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down aggregator...");
        if (consumer != null) {
            consumer.wakeup(); // прерывает poll()
        }
    }

    private void shutdownResources() {
        try {
            if (producer != null) {
                producer.flush();
                producer.close();
            }
            if (consumer != null) {
                consumer.commitSync();
                consumer.close();
            }
            log.info("Aggregator STOPPED");
        } catch (Exception e) {
            log.warn("Error during shutdown", e);
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
            throw new RuntimeException("Failed to serialize Avro: " + avro.getClass().getSimpleName(), e);
        }
    }
}