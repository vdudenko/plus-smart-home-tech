package ru.yandex.practicum.collector.grpc;

import io.grpc.stub.StreamObserver;
import com.google.protobuf.Empty;
import telemetry.service.collector.CollectHubEventRequest;
import telemetry.service.collector.CollectSensorEventRequest;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.collector.service.CollectorService;
import telemetry.service.collector.CollectorControllerGrpc.CollectorControllerImplBase;

@GrpcService
@RequiredArgsConstructor
public class CollectorGrpcService extends CollectorControllerImplBase {

    private final CollectorService collectorService;

    @Override
    public void collectHubEvent(CollectHubEventRequest request, StreamObserver<Empty> responseObserver) {
        collectorService.handleHubEvent(request);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void collectSensorEvent(CollectSensorEventRequest request, StreamObserver<Empty> responseObserver) {
        collectorService.handleSensorEvent(request);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
