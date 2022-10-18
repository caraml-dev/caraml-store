package dev.caraml.store.api.compat;

import dev.caraml.store.protobuf.compat.JobServiceGrpc;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.GetHistoricalFeaturesRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.GetHistoricalFeaturesResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.GetJobRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.GetJobResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.Job;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.StartOfflineToOnlineIngestionJobRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.StartOfflineToOnlineIngestionJobResponse;
import dev.caraml.store.sparkjob.JobNotFoundException;
import dev.caraml.store.sparkjob.JobService;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

// Endpoint to support backward compatibility for users who still use feast
// as opposed to feast_spark client
@Slf4j
@GrpcService
public class LegacyJobGrpcServiceImpl extends JobServiceGrpc.JobServiceImplBase {
  private final JobService jobService;

  @Autowired
  public LegacyJobGrpcServiceImpl(JobService jobService) {
    this.jobService = jobService;
  }

  @Override
  public void startOfflineToOnlineIngestionJob(
      StartOfflineToOnlineIngestionJobRequest request,
      StreamObserver<StartOfflineToOnlineIngestionJobResponse> responseObserver) {
    Job job =
        jobService.createOrUpdateBatchIngestionJob(
            request.getProject(),
            request.getTableName(),
            request.getStartDate(),
            request.getEndDate(),
            request.getDeltaIngestion());
    StartOfflineToOnlineIngestionJobResponse response =
        StartOfflineToOnlineIngestionJobResponse.newBuilder()
            .setJobStartTime(job.getStartTime())
            .setId(job.getId())
            .setTableName(request.getTableName())
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getHistoricalFeatures(
      GetHistoricalFeaturesRequest request,
      StreamObserver<GetHistoricalFeaturesResponse> responseObserver) {
    Job job =
        jobService.createRetrievalJob(
            request.getProject(),
            request.getFeatureRefsList(),
            request.getEntitySource(),
            request.getOutputFormat(),
            request.getOutputLocation());
    GetHistoricalFeaturesResponse response =
        GetHistoricalFeaturesResponse.newBuilder()
            .setId(job.getId())
            .setJobStartTime(job.getStartTime())
            .setOutputFileUri(request.getOutputLocation())
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getJob(GetJobRequest request, StreamObserver<GetJobResponse> responseObserver) {
    GetJobResponse response =
        jobService
            .getJob(request.getJobId())
            .map(job -> GetJobResponse.newBuilder().setJob(job).build())
            .orElseThrow(() -> new JobNotFoundException(request.getJobId()));
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
