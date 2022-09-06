package dev.caraml.serving.store.redis;

import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest.EntityRow;
import dev.caraml.store.protobuf.types.ValueProto.Value;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;

public class RedisKeyGenerator {

  public static List<byte[]> buildRedisKeys(String project, List<EntityRow> entityRows) {
    List<byte[]> redisKeys =
        entityRows.stream()
            .map(entityRow -> makeRedisKey(project, entityRow))
            .collect(Collectors.toList());

    return redisKeys;
  }

  /**
   * @param project Project where request for features was called from
   * @param entityRow {@link EntityRow}
   * @return {byte[]}
   */
  private static byte[] makeRedisKey(String project, EntityRow entityRow) {
    Map<String, Value> fieldsMap = entityRow.getFieldsMap();
    List<String> entityNames = new ArrayList<>(new HashSet<>(fieldsMap.keySet()));

    // Sort entity names by alphabetical order
    entityNames.sort(String::compareTo);

    return DigestUtils.md5Hex(
            String.format(
                "%s#%s:%s",
                project,
                String.join("#", entityNames),
                entityNames.stream()
                    .map(entity -> feastValueToString(fieldsMap.get(entity)))
                    .collect(Collectors.joining("#"))))
        .getBytes();
  }

  private static String feastValueToString(Value value) {
    String stringRepr = "";
    switch (value.getValCase()) {
      case STRING_VAL:
        stringRepr = value.getStringVal();
        break;
      case INT32_VAL:
        stringRepr = Integer.valueOf(value.getInt32Val()).toString();
        break;
      case INT64_VAL:
        stringRepr = Long.valueOf(value.getInt64Val()).toString();
        break;
    }
    return stringRepr;
  }
}
