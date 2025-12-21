package ru.yandex.practicum.collector.grpc;

import io.grpc.stub.StreamObserver;
import com.google.protobuf.Empty;
import smart_home.collector.v1.CollectHubEventRequest;
import smart_home.collector.v1.CollectSensorEventRequest;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.collector.service.CollectorService;
import smart_home.collector.v1.CollectorServiceGrpc.CollectorServiceImplBase;

@GrpcService
@RequiredArgsConstructor
public class CollectorGrpcService extends CollectorServiceImplBase {

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
