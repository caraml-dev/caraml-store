package dev.caraml.store.api;

import dev.caraml.store.protobuf.jobservice.JobServiceGrpc;
import dev.caraml.store.protobuf.jobservice.JobServiceProto;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.CancelJobRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.CancelJobResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.GetHistoricalFeaturesRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.GetHistoricalFeaturesResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.GetJobRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.GetJobResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.Job;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.ListJobsRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.ListJobsResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.ScheduleOfflineToOnlineIngestionJobRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.ScheduleOfflineToOnlineIngestionJobResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.ScheduledJob;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.StartOfflineToOnlineIngestionJobRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.StartOfflineToOnlineIngestionJobResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.StartStreamIngestionJobRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.StartStreamIngestionJobResponse;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.UnscheduleJobRequest;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.UnscheduleJobResponse;
import dev.caraml.store.sparkjob.BatchJobRecord;
import dev.caraml.store.sparkjob.JobNotFoundException;
import dev.caraml.store.sparkjob.JobService;
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
    this.jobService.startWatcher();
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
  public void startStreamIngestionJob(
      StartStreamIngestionJobRequest request,
      StreamObserver<StartStreamIngestionJobResponse> responseObserver) {
    Job job =
        jobService.createOrUpdateStreamingIngestionJob(
            request.getProject(), request.getTableName());
    StartStreamIngestionJobResponse response =
        StartStreamIngestionJobResponse.newBuilder().setId(job.getId()).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void scheduleOfflineToOnlineIngestionJob(
      ScheduleOfflineToOnlineIngestionJobRequest request,
      StreamObserver<ScheduleOfflineToOnlineIngestionJobResponse> responseObserver) {
    jobService.scheduleBatchIngestionJob(
        request.getProject(),
        request.getTableName(),
        request.getCronSchedule(),
        request.getIngestionTimespan());
    ScheduleOfflineToOnlineIngestionJobResponse response =
        ScheduleOfflineToOnlineIngestionJobResponse.getDefaultInstance();
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
  public void listJobs(ListJobsRequest request, StreamObserver<ListJobsResponse> responseObserver) {
    List<Job> jobs =
        jobService.listJobs(
            request.getIncludeTerminated(),
            request.getProject(),
            request.getTableName(),
            request.getType());
    ListJobsResponse response = ListJobsResponse.newBuilder().addAllJobs(jobs).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void listScheduledJobs(
      JobServiceProto.ListScheduledJobsRequest request,
      StreamObserver<JobServiceProto.ListScheduledJobsResponse> responseObserver) {
    List<ScheduledJob> jobs =
        jobService.listScheduledJobs(request.getProject(), request.getTableName());
    JobServiceProto.ListScheduledJobsResponse response =
        JobServiceProto.ListScheduledJobsResponse.newBuilder().addAllJobs(jobs).build();
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

  @Override
  public void cancelJob(
      CancelJobRequest request, StreamObserver<CancelJobResponse> responseObserver) {
    jobService.cancelJob(request.getJobId());
    responseObserver.onNext(CancelJobResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  public void unscheduleJob(
      UnscheduleJobRequest request, StreamObserver<UnscheduleJobResponse> responseObserver) {
    jobService.unscheduleJob(request.getJobId());
    responseObserver.onNext(UnscheduleJobResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void listBatchJobRecords(JobServiceProto.ListBatchJobRecordsRequest request, StreamObserver<JobServiceProto.ListBatchJobRecordsResponse> responseObserver){
    List<JobServiceProto.BatchJobRecord> records =
        jobService.listBatchJobRecords(request.getProject(), request.getType(), request.getTableName(), request.getFrom().getSeconds(), request.getTo().getSeconds());
    JobServiceProto.ListBatchJobRecordsResponse response =
        JobServiceProto.ListBatchJobRecordsResponse.newBuilder().addAllRecords(records).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
