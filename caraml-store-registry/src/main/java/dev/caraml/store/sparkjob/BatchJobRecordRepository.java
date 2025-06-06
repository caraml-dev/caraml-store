package dev.caraml.store.sparkjob;

import org.springframework.data.jpa.repository.JpaRepository;

public interface BatchJobRecordRepository extends JpaRepository<BatchJobRecord, String> {


}
