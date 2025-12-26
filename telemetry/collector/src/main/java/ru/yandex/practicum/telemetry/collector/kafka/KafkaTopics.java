package ru.yandex.practicum.telemetry.collector.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaTopics {
    @Value("${kafka.topic.sensors}")
    private String sensorTopic;

    @Value("${kafka.topic.hubs}")
    private String hubTopic;

    public String sensor() {
        return sensorTopic;
    }

    public String hub() {
        return hubTopic;
    }
}