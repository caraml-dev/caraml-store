package dev.caraml.serving.store.redis;

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import dev.caraml.serving.store.Feature;
import dev.caraml.serving.store.OnlineRetriever;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest.EntityRow;
import io.grpc.Status;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RedisOnlineRetriever implements OnlineRetriever {

  private static final String timestampPrefix = "_ts";
  private RedisClientAdapter redisClientAdapter;

  public RedisOnlineRetriever(RedisClientAdapter redisClientAdapter) {
    this.redisClientAdapter = redisClientAdapter;
  }

  @Override
  public List<List<Feature>> getOnlineFeatures(
      String project,
      List<EntityRow> entityRows,
      List<FeatureReference> featureReferences,
      List<String> entityNames) {

    List<byte[]> redisKeys = RedisKeyGenerator.buildRedisKeys(project, entityRows);
    return getFeaturesFromRedis(redisKeys, featureReferences);
  }

  private List<List<Feature>> getFeaturesFromRedis(
      List<byte[]> redisKeys, List<FeatureReference> featureReferences) {
    List<List<Feature>> features = new ArrayList<>();
    // To decode bytes back to Feature Reference
    Map<String, FeatureReference> byteToFeatureReferenceMap = new HashMap<>();
    Map<String, String> byteToFeatureTableTsMap = new HashMap<>();

    List<byte[]> featureReferenceWithTsByteList = new ArrayList<>();
    featureReferences.stream()
        .forEach(
            featureReference -> {

              // eg. murmur(<featuretable_name:feature_name>)
              byte[] featureReferenceBytes =
                  RedisHashDecoder.getFeatureReferenceRedisHashKeyBytes(featureReference);
              featureReferenceWithTsByteList.add(featureReferenceBytes);
              byteToFeatureReferenceMap.put(featureReferenceBytes.toString(), featureReference);

              // eg. murmur(<_ts:featuretable_name>)
              byte[] featureTableTsBytes =
                  RedisHashDecoder.getTimestampRedisHashKeyBytes(featureReference, timestampPrefix);
              featureReferenceWithTsByteList.add(featureTableTsBytes);
              byteToFeatureTableTsMap.put(
                  featureTableTsBytes.toString(),
                  timestampPrefix + ":" + featureReference.getFeatureTable());
            });

    // Perform a series of independent calls
    List<RedisFuture<List<KeyValue<byte[], byte[]>>>> futures = Lists.newArrayList();
    for (byte[] redisKey : redisKeys) {
      byte[][] featureReferenceWithTsByteArrays =
          featureReferenceWithTsByteList.toArray(new byte[0][]);
      // Access redis keys and extract features
      futures.add(redisClientAdapter.hmget(redisKey, featureReferenceWithTsByteArrays));
    }

    // Write all commands to the transport layer
    // redisClientAdapter.flushCommands();

    futures.forEach(
        future -> {
          try {
            List<KeyValue<byte[], byte[]>> redisValuesList = future.get();
            List<Feature> curRedisKeyFeatures =
                RedisHashDecoder.retrieveFeature(
                    redisValuesList,
                    byteToFeatureReferenceMap,
                    byteToFeatureTableTsMap,
                    timestampPrefix);
            features.add(curRedisKeyFeatures);
          } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
            throw Status.UNKNOWN
                .withDescription("Unexpected error when pulling data from from Redis.")
                .withCause(e)
                .asRuntimeException();
          }
        });
    return features;
  }
}
