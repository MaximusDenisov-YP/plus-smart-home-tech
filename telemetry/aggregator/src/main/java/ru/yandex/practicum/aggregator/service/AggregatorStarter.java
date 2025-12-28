package ru.yandex.practicum.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.kafka.KafkaTopicConfiguration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.Optional;

@Slf4j
@Component
public class AggregatorStarter {

    private final KafkaProducer<String, SensorsSnapshotAvro> producer;
    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final SnapshotService snapshotService;
    private final KafkaTopicConfiguration topics;

    public AggregatorStarter(
            KafkaProducer<String, SensorsSnapshotAvro> producer,
            KafkaConsumer<String, SensorEventAvro> consumer,
            SnapshotService snapshotService,
            KafkaTopicConfiguration topics
    ) {
        this.producer = producer;
        this.consumer = consumer;
        this.snapshotService = snapshotService;
        this.topics = topics;
    }

    public void start() {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::wakeup));
            consumer.subscribe(topics.consumerTopic());
            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, SensorEventAvro> record : records) {
                        log.info(
                                "Processing  ConsumerRecord: topic={}, partition={}, offset={}, hubId={}, timestamp={}",
                                record.topic(), record.partition(), record.offset(), record.key(), record.timestamp());

                        final Optional<SensorsSnapshotAvro> updatedSnapshot = snapshotService.updateState(
                                record.value());
                        updatedSnapshot.ifPresent(this::sendSnapshotToKafka);
                    }
                    consumer.commitSync();
                }
            }
        } catch (WakeupException ignored) {
            log.warn("Получено исключение WakeupException - Consumer выключен");
        } catch (Exception e) {
            log.error("Произошла ошибка в процессе отправки события", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } catch (Exception e) {
                log.error("Произошла ошибка в завершении процесса", e);
            } finally {
                consumer.close();
                producer.close();
            }
        }
    }

    private void sendSnapshotToKafka(SensorsSnapshotAvro snapshot) {
        log.info("Отправка snapshot в Kafka: hubId={}", snapshot.getHubId());
        final ProducerRecord<String, SensorsSnapshotAvro> record =
                new ProducerRecord<>(topics.producerTopic(), snapshot.getHubId(), snapshot);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка отправки snapshot в Kafka", exception);
            } else {
                log.info("Snapshot успешно отправлен в топик {} оффсет = {}", metadata.topic(), metadata.offset());
            }
        });
    }
}