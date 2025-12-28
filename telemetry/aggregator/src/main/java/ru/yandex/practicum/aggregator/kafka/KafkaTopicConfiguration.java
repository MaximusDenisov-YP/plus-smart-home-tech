package ru.yandex.practicum.aggregator.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Slf4j
@Configuration
public class KafkaTopicConfiguration {

    @Value("${aggregator.kafka.topics.consumer-subscription}")
    private List<String> consumerSubscription;

    @Value("${aggregator.kafka.topics.producer-topic}")
    private String producerTopic;

    public String producerTopic() {
        log.debug("Агрегатору настроен топик продюсера: {}", producerTopic);
        return producerTopic;
    }

    public List<String> consumerTopic() {
        log.debug("Агрегатору настроен топик консьюмера: {}", consumerSubscription);
        return consumerSubscription;
    }
}