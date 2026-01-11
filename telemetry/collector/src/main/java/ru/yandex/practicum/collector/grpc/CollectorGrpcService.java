package ru.yandex.practicum.collector.grpc;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.practicum.collector.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;


public class CollectorGrpcService extends CollectorControllerGrpc.CollectorControllerImplBase {

    private static final Logger log = LoggerFactory.getLogger(CollectorGrpcService.class);

    private final GrpcService collectorService;

    public CollectorGrpcService(GrpcService collectorService) {
        this.collectorService = collectorService;
    }

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.debug("Получено событие от датчика: {}", request);
            collectorService.handleSensorEvent(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка при обработке события от датчика", e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.debug("Получено событие от хаба: {}", request);
            collectorService.handleHubEvent(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка при обработке события от хаба", e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }
}
