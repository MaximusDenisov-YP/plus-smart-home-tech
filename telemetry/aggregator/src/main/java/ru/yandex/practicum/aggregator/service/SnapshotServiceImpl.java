package ru.yandex.practicum.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
public class SnapshotServiceImpl implements SnapshotService {

    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    @Override
    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        Optional<SensorsSnapshotAvro> optSensorsSnapshotAvro = Optional
                .ofNullable(snapshots.get(event.getHubId()));
        final SensorsSnapshotAvro snapshotAvro = optSensorsSnapshotAvro.orElseGet(
                () -> SensorsSnapshotAvro.newBuilder()
                        .setHubId(event.getHubId())
                        .setTimestamp(event.getTimestamp())
                        .setSensorsState(new HashMap<>())
                        .build()
        );

        if (isActualSnapshotValid(snapshotAvro, event)) return Optional.empty();

        final SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        snapshotAvro.getSensorsState().put(event.getId(), newState);
        snapshotAvro.setTimestamp(event.getTimestamp());

        snapshots.put(snapshotAvro.getHubId(), snapshotAvro);
        return Optional.of(snapshotAvro);
    }

    public boolean isActualSnapshotValid(
            final SensorsSnapshotAvro actualSnapshot,
            final SensorEventAvro event
    ) {
        final SensorStateAvro actualState = actualSnapshot.getSensorsState().get(event.getHubId());
        return actualState != null &&
                (!actualState.getTimestamp().isBefore(event.getTimestamp()) ||
                        actualState.getData().equals(event.getPayload()));
    }
}
