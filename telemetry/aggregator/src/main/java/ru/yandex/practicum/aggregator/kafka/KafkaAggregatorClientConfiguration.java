package ru.yandex.practicum.aggregator.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

@Configuration
public class KafkaAggregatorClientConfiguration implements KafkaAggregatorClient {

    @Bean
    public KafkaProducer<String, SensorsSnapshotAvro> getProducer(
            @Value("${aggregator.kafka.producer.bootstrap-server}") String bootstrapProducerServer,
            @Value("${aggregator.kafka.producer.key-serializer}") String keyProducerSerializer,
            @Value("${aggregator.kafka.producer.value-serializer}") String valueProducerSerializer
    ) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapProducerServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keyProducerSerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueProducerSerializer);
        return new KafkaProducer<>(properties);
    }

    @Bean
    public KafkaConsumer<String, SensorEventAvro> getConsumer(
            @Value("${aggregator.kafka.consumer.bootstrap-server}") String bootstrapConsumerServer,
            @Value("${aggregator.kafka.consumer.group-id}") String groupId,
            @Value("${aggregator.kafka.consumer.auto-offset-reset}") String autoOffsetReset,
            @Value("${aggregator.kafka.consumer.enable-auto-commit}") boolean enableAutoCommit,
            @Value("${aggregator.kafka.consumer.key-deserializer}") String keyConsumerDeserializer,
            @Value("${aggregator.kafka.consumer.value-deserializer}") String valueConsumerDeserializer
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapConsumerServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyConsumerDeserializer);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueConsumerDeserializer);
        return new KafkaConsumer<>(properties);
    }
}
