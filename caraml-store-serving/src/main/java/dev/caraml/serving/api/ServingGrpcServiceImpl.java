package dev.caraml.serving.api;

import dev.caraml.serving.monitoring.MonitoringInterceptor;
import dev.caraml.serving.store.StoreService;
import dev.caraml.store.protobuf.serving.ServingServiceGrpc;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetFeastServingInfoRequest;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetFeastServingInfoResponse;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesResponse;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesResponseV2;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;

@Getter
@GrpcService(interceptors = {MonitoringInterceptor.class})
public class ServingGrpcServiceImpl extends ServingServiceGrpc.ServingServiceImplBase {

  private final StoreService storeService;
  private final String version;

  @Autowired
  public ServingGrpcServiceImpl(StoreService storeService, BuildProperties buildProperties) {
    version = buildProperties.getVersion();
    this.storeService = storeService;
  }

  @Override
  public void getFeastServingInfo(
      GetFeastServingInfoRequest request,
      StreamObserver<GetFeastServingInfoResponse> responseObserver) {
    GetFeastServingInfoResponse response =
        GetFeastServingInfoResponse.newBuilder().setVersion(getVersion()).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getOnlineFeaturesV2(
      GetOnlineFeaturesRequest request,
      StreamObserver<GetOnlineFeaturesResponseV2> responseObserver) {
    GetOnlineFeaturesResponseV2 response = storeService.getOnlineFeaturesV2(request);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getOnlineFeatures(
      GetOnlineFeaturesRequest request,
      StreamObserver<GetOnlineFeaturesResponse> responseObserver) {
    GetOnlineFeaturesResponse response = storeService.getOnlineFeatures(request);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
