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

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubRouterClient {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub stub;

    public void sendAction(String hubId, String scenarioName, DeviceActionAvro action, long timestamp) {
        var request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioName)
                .setAction(toProto(action))
                .setTimestamp(toProtoTimestamp(timestamp))
                .build();

        int maxRetries = 10;
        long delayMs = 200;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                stub.withDeadlineAfter(3, TimeUnit.SECONDS).handleDeviceAction(request);
                log.debug("Action sent to hub {}: {}", hubId, action);
                return;
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE && attempt < maxRetries) {
                    // НЕ логируем как ошибку — это ожидаемое поведение при запуске
                    log.debug("Hub Router unavailable (attempt {}/{}), retrying in {} ms",
                            attempt, maxRetries, delayMs);
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                } else {
                    // Только настоящие ошибки логируем как ERROR
                    log.error("Failed to send action to hub {} after {} attempts: {}",
                            hubId, maxRetries, e.getMessage());
                    return;
                }
            }
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