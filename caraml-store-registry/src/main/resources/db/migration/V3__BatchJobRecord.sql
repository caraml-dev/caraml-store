-- Create table for BatchJobRecord
CREATE TABLE batch_job_records
(
    id                       BIGINT NOT NULL,
    ingestion_job_id         VARCHAR(255),
    job_type                 INTEGER NOT NULL,
    project                  VARCHAR(255) NOT NULL,
    feature_table_id         BIGINT,
    status                   VARCHAR(255),
    job_start_time           BIGINT,
    job_end_time             BIGINT,
    start_time               TIMESTAMP WITHOUT TIME ZONE,
    end_time                 TIMESTAMP WITHOUT TIME ZONE,
    spark_application_manifest TEXT,
    CONSTRAINT pk_batch_job_records PRIMARY KEY (id),
    CONSTRAINT fk_batch_job_records_feature_table FOREIGN KEY (feature_table_id) REFERENCES feature_tables (id)
);