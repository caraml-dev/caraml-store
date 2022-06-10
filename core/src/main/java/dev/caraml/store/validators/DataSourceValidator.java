package dev.caraml.store.validators;

import dev.caraml.store.protobuf.core.DataFormatProto.FileFormat;
import dev.caraml.store.protobuf.core.DataFormatProto.StreamFormat;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;

public class DataSourceValidator {
  /** Validate if the given DataSource protobuf spec is valid. */
  public static void validate(DataSource spec) {
    switch (spec.getType()) {
      case BATCH_FILE -> {
        FileFormat.FormatCase fileFormat = spec.getFileOptions().getFileFormat().getFormatCase();
        if (fileFormat != FileFormat.FormatCase.PARQUET_FORMAT) {
          throw new UnsupportedOperationException(
              String.format("Unsupported File Format: %s", fileFormat));
        }
      }
      case BATCH_BIGQUERY -> Matchers.checkValidBigQueryTableRef(
          spec.getBigqueryOptions().getTableRef(), "FeatureTable");
      case STREAM_KAFKA -> {
        StreamFormat.FormatCase messageFormat =
            spec.getKafkaOptions().getMessageFormat().getFormatCase();
        switch (messageFormat) {
          case PROTO_FORMAT:
            Matchers.checkValidClassPath(
                spec.getKafkaOptions().getMessageFormat().getProtoFormat().getClassPath(),
                "FeatureTable");
            break;
          case AVRO_FORMAT:
            break;
          default:
            throw new UnsupportedOperationException(
                String.format(
                    "Unsupported Stream Format for Kafka Source Type: %s", messageFormat));
        }
      }
      default -> throw new UnsupportedOperationException(
          String.format("Unsupported Feature Store Type: %s", spec.getType()));
    }
  }
}
