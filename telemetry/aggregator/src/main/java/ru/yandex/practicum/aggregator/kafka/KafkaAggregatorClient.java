package ru.yandex.practicum.aggregator.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

public interface KafkaAggregatorClient {

    Producer<String, SensorsSnapshotAvro> getProducer(
            String bootstrapServers,
            String keySerializer,
            String valueSerializer
    );

    Consumer<String, SensorEventAvro> getConsumer(
            String bootstrapServers,
            String groupId,
            String autoOffsetReset,
            boolean enableAutoCommit,
            String keyDeserializer,
            String valueDeserializer
    );
}