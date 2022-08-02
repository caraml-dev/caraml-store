package dev.caraml.store.sparkjob;

import com.google.protobuf.Timestamp;
import dev.caraml.store.protobuf.jobservice.JobServiceGrpc;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.*;
import dev.caraml.store.sparkjob.crd.SparkApplication;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@GrpcService
public class JobGrpcServiceImpl extends JobServiceGrpc.JobServiceImplBase {
  private final JobService jobService;

  @Autowired
  public JobGrpcServiceImpl(JobService jobService) {
    this.jobService = jobService;
  }

  @Override
  public void startOfflineToOnlineIngestionJob(
      StartOfflineToOnlineIngestionJobRequest request,
      StreamObserver<StartOfflineToOnlineIngestionJobResponse> responseObserver) {
    SparkApplication sparkApplication =
        jobService.createOrUpdateBatchIngestionJob(
            request.getProject(),
            request.getTableName(),
            request.getStartDate(),
            request.getEndDate());
    StartOfflineToOnlineIngestionJobResponse response =
        StartOfflineToOnlineIngestionJobResponse.newBuilder()
            .setJobStartTime(
                Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
            .setId(sparkApplication.getMetadata().getName())
            .setTableName(request.getTableName())
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
