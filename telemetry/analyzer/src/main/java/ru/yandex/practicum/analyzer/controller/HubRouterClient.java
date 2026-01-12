package ru.yandex.practicum.analyzer.controller;

import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.hubrouter.DeviceActionRequest;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubRouterClient {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub stub;

    public void sendAction(String hubId, String scenarioName, DeviceActionAvro action, long timestamp) {
        try {
            var request = DeviceActionRequest.newBuilder()
                    .setHubId(hubId)
                    .setScenarioName(scenarioName)
                    .setAction(toProto(action))
                    .setTimestamp(toProtoTimestamp(timestamp))
                    .build();

            stub.handleDeviceAction(request);
            log.debug("Action sent to hub {}: {}", hubId, action);
        } catch (StatusRuntimeException e) {
            log.error("Failed to send action to hub {}: {}", hubId, e.getMessage(), e);
        }
    }

    private DeviceActionProto toProto(DeviceActionAvro avro) {
        return DeviceActionProto.newBuilder()
                .setSensorId(avro.getSensorId())
                .setType(toProtoActionType(avro.getType())) // ← Правильное преобразование enum → enum
                .setValue(avro.getValue())
                .build();
    }

    private ActionTypeProto toProtoActionType(ActionTypeAvro avroType) {
        return switch (avroType) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };
    }

    private com.google.protobuf.Timestamp toProtoTimestamp(long timestampMs) {
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(timestampMs / 1000)
                .setNanos((int) (timestampMs % 1000) * 1_000_000)
                .build();
    }
}