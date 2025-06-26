package dev.caraml.store.sparkjob;

import dev.caraml.store.feature.FeatureTable;
import dev.caraml.store.protobuf.jobservice.JobServiceProto;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BatchJobRecordRepository extends JpaRepository<BatchJobRecord, String> {
  List<BatchJobRecord>
      findAllByJobTypeAndProjectAndFeatureTableAndJobStartTimeBetweenOrderByJobStartTimeDesc(
          JobServiceProto.JobType jobType,
          String project,
          FeatureTable featureTable,
          long startTime,
          long endTime);
}
