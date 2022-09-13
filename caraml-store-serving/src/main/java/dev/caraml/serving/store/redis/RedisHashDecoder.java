package dev.caraml.serving.store.redis;

import com.google.common.hash.Hashing;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import dev.caraml.serving.store.Feature;
import dev.caraml.serving.store.ProtoFeature;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.types.ValueProto.Value;
import io.lettuce.core.KeyValue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisHashDecoder {

  /**
   * Converts all retrieved Redis Hash values based on EntityRows into {@link Feature}
   *
   * @param redisHashValues retrieved Redis Hash values based on EntityRows
   * @param byteToFeatureReferenceMap map to decode bytes back to FeatureReference
   * @param byteToFeatureTableTsMap map to decode bytes back to FeatureTableTimestampKey
   * @param timestampPrefix prefix for timestamp key
   * @return List of {@link Feature}
   * @throws InvalidProtocolBufferException protocol message being parsed is invalid
   */
  public static List<Feature> retrieveFeature(
      List<KeyValue<byte[], byte[]>> redisHashValues,
      Map<String, FeatureReference> byteToFeatureReferenceMap,
      Map<String, String> byteToFeatureTableTsMap,
      String timestampPrefix)
      throws InvalidProtocolBufferException {
    List<Feature> allFeatures = new ArrayList<>();
    HashMap<FeatureReference, Value> featureMap = new HashMap<>();
    Map<String, Timestamp> featureTableTimestampMap = new HashMap<>();

    for (KeyValue<byte[], byte[]> entity : redisHashValues) {
      if (entity.hasValue()) {
        byte[] redisValueK = entity.getKey();
        byte[] redisValueV = entity.getValue();
        String redisValueKey = redisValueK.toString();
        // Decode data from Redis into Feature object fields
        if (byteToFeatureReferenceMap.containsKey(redisValueKey)) {
          FeatureReference featureReference = byteToFeatureReferenceMap.get(redisValueKey);
          Value featureValue = Value.parseFrom(redisValueV);

          featureMap.put(featureReference, featureValue);
        } else {
          // key should be present in byteToFeatureTableTsMap
          String featureTableTsKey = byteToFeatureTableTsMap.get(redisValueKey);
          Timestamp eventTimestamp = Timestamp.parseFrom(redisValueV);

          featureTableTimestampMap.put(featureTableTsKey, eventTimestamp);
        }
      }
    }

    // Add timestamp to features
    for (Map.Entry<FeatureReference, Value> entry : featureMap.entrySet()) {
      String timestampRedisHashKeyStr = timestampPrefix + ":" + entry.getKey().getFeatureTable();
      Timestamp curFeatureTimestamp = featureTableTimestampMap.get(timestampRedisHashKeyStr);

      ProtoFeature curFeature =
          new ProtoFeature(entry.getKey(), curFeatureTimestamp, entry.getValue());
      allFeatures.add(curFeature);
    }

    return allFeatures;
  }

  public static byte[] getTimestampRedisHashKeyBytes(
      FeatureReference featureReference, String timestampPrefix) {
    String timestampRedisHashKeyStr = timestampPrefix + ":" + featureReference.getFeatureTable();
    return Hashing.murmur3_32()
        .hashString(timestampRedisHashKeyStr, StandardCharsets.UTF_8)
        .asBytes();
  }

  public static byte[] getFeatureReferenceRedisHashKeyBytes(FeatureReference featureReference) {
    String delimitedFeatureReference =
        featureReference.getFeatureTable() + ":" + featureReference.getName();
    return Hashing.murmur3_32()
        .hashString(delimitedFeatureReference, StandardCharsets.UTF_8)
        .asBytes();
  }
}
