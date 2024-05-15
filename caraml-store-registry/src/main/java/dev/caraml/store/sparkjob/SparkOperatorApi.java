package dev.caraml.store.sparkjob;

import dev.caraml.store.sparkjob.crd.ScheduledSparkApplication;
import dev.caraml.store.sparkjob.crd.SparkApplication;
import java.util.List;
import java.util.Optional;

public interface SparkOperatorApi {

  SparkApplication update(SparkApplication app) throws SparkOperatorApiException;

  SparkApplication create(SparkApplication app) throws SparkOperatorApiException;

  ScheduledSparkApplication update(ScheduledSparkApplication app) throws SparkOperatorApiException;

  ScheduledSparkApplication create(ScheduledSparkApplication app) throws SparkOperatorApiException;

  List<SparkApplication> list(String namespace, String labelSelector)
      throws SparkOperatorApiException;

  Optional<SparkApplication> getSparkApplication(String namespace, String name)
      throws SparkOperatorApiException;

  List<ScheduledSparkApplication> listScheduled(String namespace, String labelSelector)
      throws SparkOperatorApiException;

  Optional<ScheduledSparkApplication> getScheduledSparkApplication(String namespace, String name)
      throws SparkOperatorApiException;
}
