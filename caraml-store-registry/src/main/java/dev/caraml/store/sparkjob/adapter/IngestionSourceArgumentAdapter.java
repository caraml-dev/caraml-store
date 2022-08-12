package dev.caraml.store.sparkjob.adapter;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import dev.caraml.store.protobuf.core.DataSourceProto.DataSource;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor
class IngestionSourceArgumentAdapter implements SparkApplicationArgumentAdapter {

  private DataSource source;

  @Override
  public List<String> getArguments() {
    try {
      return List.of(
          "--source", JsonFormat.printer().omittingInsignificantWhitespace().print(source));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
