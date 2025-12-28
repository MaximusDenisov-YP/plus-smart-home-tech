package ru.yandex.practicum.telemetry.collector.service.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaTopics;
import ru.yandex.practicum.telemetry.collector.model.DeviceRemovedEvent;

import java.time.Instant;

@Component(value = "DEVICE_REMOVED_HUB_EVENT")
public class DeviceRemovedEventHandler extends BaseHubEventHandler<DeviceRemovedEvent> {

    public DeviceRemovedEventHandler(KafkaClient client, KafkaTopics topics) {
        super(client, topics);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    public HubEventAvro mapToAvro(HubEventProto event) {
        DeviceRemovedEventProto deviceRemovedEvent = event.getDeviceRemoved();
        DeviceRemovedEventAvro deviceRemovedEventAvro = DeviceRemovedEventAvro.newBuilder()
                .setId(deviceRemovedEvent.getId())
                .build();
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(
                        event.getTimestamp().getSeconds(),
                        event.getTimestamp().getNanos()
                ))
                .setPayload(deviceRemovedEventAvro)
                .build();
    }

}