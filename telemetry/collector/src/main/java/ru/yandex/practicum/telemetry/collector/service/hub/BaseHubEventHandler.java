package ru.yandex.practicum.telemetry.collector.service.hub;

import lombok.Getter;
import lombok.Setter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaTopics;
import ru.yandex.practicum.telemetry.collector.model.HubEvent;
import ru.yandex.practicum.telemetry.collector.service.HubEventHandler;

@Getter
@Setter
public abstract class BaseHubEventHandler<T extends HubEvent> implements HubEventHandler {
    private final KafkaClient client;
    private final KafkaTopics topics;

    public BaseHubEventHandler(KafkaClient client, KafkaTopics topics) {
        this.client = client;
        this.topics = topics;
    }

    public abstract HubEventProto.PayloadCase getMessageType();

    public abstract HubEventAvro mapToAvro(HubEventProto event);

    @Override
    public void handle(HubEventProto event) {
        HubEventAvro hubEventAvro = mapToAvro(event);
        Producer<String, SpecificRecordBase> producer = client.getProducer();
        producer.send(new ProducerRecord<>(topics.hub(), hubEventAvro));
    }

}