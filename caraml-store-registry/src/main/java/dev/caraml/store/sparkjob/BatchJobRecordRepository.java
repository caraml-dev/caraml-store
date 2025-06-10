package dev.caraml.store.sparkjob;

import dev.caraml.store.feature.FeatureTable;
import dev.caraml.store.protobuf.jobservice.JobServiceProto;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface BatchJobRecordRepository extends JpaRepository<BatchJobRecord, String> {
    List<BatchJobRecord> findAllByJobTypeAndProjectAndFeatureTableAndJobStartTimeBetween(
            JobServiceProto.JobType jobType, String project, FeatureTable featureTable, long startTime, long endTime);
}
