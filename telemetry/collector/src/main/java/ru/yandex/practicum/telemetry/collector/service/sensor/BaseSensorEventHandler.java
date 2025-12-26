package ru.yandex.practicum.telemetry.collector.service.sensor;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaTopics;
import ru.yandex.practicum.telemetry.collector.model.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.SensorEventHandler;

@Getter
@Setter
public abstract class BaseSensorEventHandler<T extends SensorEvent> implements SensorEventHandler {
    private final KafkaClient client;
    private final KafkaTopics topics;

    public BaseSensorEventHandler(KafkaClient client, KafkaTopics topics) {
        this.client = client;
        this.topics = topics;
    }

    public abstract SensorEventType getMessageType();

    public abstract SensorEventAvro mapToAvro(SensorEvent event);

    @Override
    public void handle(SensorEvent event) {
        SensorEventAvro sensorEventAvro = mapToAvro(event);
        Producer<String, SpecificRecordBase> producer = client.getProducer();
        producer.send(new ProducerRecord<>(topics.sensor(), sensorEventAvro));
    }

}