package ru.yandex.practicum.telemetry.collector.service.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaTopics;
import ru.yandex.practicum.telemetry.collector.model.LightSensorEvent;

import java.time.Instant;

@Component(value = "LIGHT_SENSOR_EVENT")
public class LightSensorEventHandler extends BaseSensorEventHandler<LightSensorEvent> {

    public LightSensorEventHandler(KafkaClient client, KafkaTopics topics) {
        super(client, topics);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR;
    }

    @Override
    public SensorEventAvro mapToAvro(SensorEventProto event) {
        LightSensorProto lightSensorEvent = event.getLightSensor();
        LightSensorEventAvro lightSensorEventAvro = LightSensorEventAvro.newBuilder()
                .setLinkQuality(lightSensorEvent.getLinkQuality())
                .setLuminosity(lightSensorEvent.getLuminosity())
                .build();
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(
                        event.getTimestamp().getSeconds(),
                        event.getTimestamp().getNanos()
                ))
                .setPayload(lightSensorEventAvro)
                .build();
    }

}