package dev.caraml.store.sparkjob;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.apache.commons.lang3.tuple.Pair;

import dev.caraml.store.feature.FeatureTable;
import dev.caraml.store.protobuf.jobservice.JobServiceProto.JobType;
import dev.caraml.store.sparkjob.crd.SparkApplication;

import java.sql.Timestamp;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Entity
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "batch_job_records")
public class BatchJobRecord {

    @Column(name = "id")
    @Id
    @GeneratedValue
    private long id;

    private String ingestionJobId;

    @Column(name = "job_type", nullable = false)
    // The type of the ingestion job (e.g., BATCH, RETRIEVAL)
    private JobType jobType;

    // The project associated with the ingestion job
    @Column(name = "project", nullable = false)
    private String project;

    // The feature table associated with the ingestion job
    @ManyToOne(optional = true)
    private FeatureTable featureTable;

    // The status of the ingestion job (e.g., RUNNING, COMPLETED, FAILED)
    @Column(name = "status")
    private String status;

    // The start time of the ingestion job
    @Column(name = "job_start_time")
    private long jobEndTime;

    // The end time of the ingestion job
    @Column(name = "job_end_time")
    private long jobStartTime;

    // start parameters passed into spark job
    @Column(name = "start_time")
    private Timestamp startTime;

    // end parameters passed into spark job
    @Column(name = "end_time")
    private Timestamp endTime;

    // // Any error message associated with the ingestion job
    // private String errorMessage;

    @Column(name = "spark_application_manifest", columnDefinition = "text")
    private String sparkApplicationManifest;

    public static Pair<Timestamp, Timestamp> getStartEndTimeParamsFromSparkApplication(
            SparkApplication sparkApplication) {

        /*
         * abstract
         * 
         * - --start
         * - "2025-05-17T00:00:00Z"
         * - --end
         * - "2025-05-18T00:00:00Z"
         */
        List<String> input = sparkApplication.getSpec().getArguments();
        String startTime = null;
        String endTime = null;
        String ingestionTimespan = null;

        for (int i = 0; i < input.size() - 1; i++) {
            if (input.get(i).equals("--start")) {
                startTime = input.get(i + 1);
            } else if (input.get(i).equals("--end")) {
                endTime = input.get(i + 1);
            } else if (input.get(i).equals("--ingestion-timespan")) {
                ingestionTimespan = input.get(i + 1);
            }
        }
        if (startTime == null || endTime == null) {
            // check for ingestion timespan
            // TODO: decide how to handle this case
            if (ingestionTimespan != null) {
                // If ingestion timespan is provided, we can derive start and end time
                // For example, if ingestionTimespan is "1d", we can set startTime to 1 day ago
                // and endTime to now.
                long currentTimeMillis = System.currentTimeMillis();
                long oneDayInMillis = 24 * 60 * 60 * 1000; // 1 day in milliseconds
                startTime = new Timestamp(currentTimeMillis - oneDayInMillis).toString();
                endTime = new Timestamp(currentTimeMillis).toString();
            } else {
                throw new IllegalArgumentException("Start and end time parameters are missing.");
            }

        }
        return Pair.of(
                Timestamp.valueOf(startTime.replace("T", " ").replace("Z", "")),
                Timestamp.valueOf(endTime.replace("T", " ").replace("Z", "")));
    }
}