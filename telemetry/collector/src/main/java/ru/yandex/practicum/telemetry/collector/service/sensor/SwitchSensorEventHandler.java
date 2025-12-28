package ru.yandex.practicum.telemetry.collector.service.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorEventAvro;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaClient;
import ru.yandex.practicum.telemetry.collector.kafka.KafkaTopics;
import ru.yandex.practicum.telemetry.collector.model.SwitchSensorEvent;

import java.time.Instant;

@Component(value = "SWITCH_SENSOR_EVENT")
public class SwitchSensorEventHandler extends BaseSensorEventHandler<SwitchSensorEvent> {

    public SwitchSensorEventHandler(KafkaClient client, KafkaTopics topics) {
        super(client, topics);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR;
    }

    @Override
    public SensorEventAvro mapToAvro(SensorEventProto event) {
        SwitchSensorProto switchSensorEvent = event.getSwitchSensor();
        SwitchSensorEventAvro switchSensorEventAvro = SwitchSensorEventAvro.newBuilder()
                .setState(switchSensorEvent.getState())
                .build();
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(
                        event.getTimestamp().getSeconds(),
                        event.getTimestamp().getNanos()
                ))
                .setPayload(switchSensorEventAvro)
                .build();
    }

}