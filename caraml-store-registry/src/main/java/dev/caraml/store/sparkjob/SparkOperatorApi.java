package dev.caraml.store.sparkjob;

import java.util.List;
import java.util.Optional;

public interface SparkOperatorApi {

  SparkApplication update(SparkApplication app) throws SparkOperatorApiException;

  SparkApplication create(SparkApplication app) throws SparkOperatorApiException;

  List<SparkApplication> list(String namespace, String labelSelector)
      throws SparkOperatorApiException;

  Optional<SparkApplication> get(String namespace, String name) throws SparkOperatorApiException;
}
