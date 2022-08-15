package dev.caraml.store.sparkjob;

import dev.caraml.store.protobuf.jobservice.JobServiceGrpc;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.Job;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.ListJobsRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.ListJobsResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.StartOfflineToOnlineIngestionJobRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.StartOfflineToOnlineIngestionJobResponse;
import io.grpc.stub.StreamObserver;
import java.util.List;
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
    Job job =
        jobService.createOrUpdateBatchIngestionJob(
            request.getProject(),
            request.getTableName(),
            request.getStartDate(),
            request.getEndDate());
    StartOfflineToOnlineIngestionJobResponse response =
        StartOfflineToOnlineIngestionJobResponse.newBuilder()
            .setJobStartTime(job.getStartTime())
            .setId(job.getId())
            .setTableName(request.getTableName())
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  public void listJobs(ListJobsRequest request, StreamObserver<ListJobsResponse> responseObserver) {
    List<Job> jobs =
        jobService.listJobs(
            request.getIncludeTerminated(), request.getProject(), request.getTableName());
    ListJobsResponse response = ListJobsResponse.newBuilder().addAllJobs(jobs).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
