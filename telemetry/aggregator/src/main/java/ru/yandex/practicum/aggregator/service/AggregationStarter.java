package ru.yandex.practicum.aggregator.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
        // Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "aggregator-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SensorEventAvroDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(consumerProps);

        // Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProps);
    }

    public void start() {
        consumer.subscribe(Collections.singletonList("telemetry.sensors.v1"));
        log.info("Aggregator started. Listening to telemetry.sensors.v1...");

        try {
            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    handleEvent(record.value());
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            log.info("Consumer woken up for shutdown");
        } catch (Exception e) {
            log.error("Error during aggregation", e);
        } finally {
            try {
                producer.flush(); // сброс буферов
                consumer.commitSync(); // фиксация оффсетов
            } finally {
                consumer.close();
                producer.close();
                log.info("Aggregator stopped");
            }
        }
    }

    private void handleEvent(SensorEventAvro event) {
        snapshotManager.updateState(event)
                .ifPresent(snapshot -> {
                    log.debug("Sending updated snapshot for hub: {}", snapshot.getHubId());
                    byte[] data = serializeAvro(snapshot);
                    producer.send(new ProducerRecord<>("telemetry.snapshots.v1", snapshot.getHubId(), data));
                });
    }

    @PreDestroy
    public void shutdown() {
        consumer.wakeup(); // прерывает poll()
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
            throw new RuntimeException("Failed to serialize Avro", e);
        }
    }
}