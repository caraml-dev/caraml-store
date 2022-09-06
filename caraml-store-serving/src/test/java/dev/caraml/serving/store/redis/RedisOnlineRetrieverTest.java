package dev.caraml.serving.store.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.protobuf.Timestamp;
import dev.caraml.serving.store.Feature;
import dev.caraml.store.protobuf.serving.ServingServiceProto;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.types.ValueProto;
import dev.caraml.store.protobuf.types.ValueProto.Value;
import dev.caraml.store.testutils.it.DataGenerator;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class RedisOnlineRetrieverTest {

  private static String REDIS_PASSWORD = "password";
  private static Integer REDIS_PORT = 6379;
  private static RedisCommands<byte[], byte[]> syncCommands;
  static final String FEAST_PROJECT = "default";

  @Container
  public static GenericContainer<?> redis =
      new GenericContainer<>("redis:6-alpine").withExposedPorts(REDIS_PORT);

  @BeforeAll
  public static void setup() {
    RedisURI.Builder uriBuilder =
        RedisURI.builder()
            .withHost("localhost")
            .withPort(redis.getMappedPort(REDIS_PORT))
            .withTimeout(Duration.ofMillis(2000));
    RedisURI uri = uriBuilder.build();

    io.lettuce.core.RedisClient redisClient = io.lettuce.core.RedisClient.create(uri);
    StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(new ByteArrayCodec());
    syncCommands = connection.sync();
    ingestData();
  }

  private static void ingestData() {
    String featureTableName = "rides";
    FeatureReference feature1Reference =
        DataGenerator.createFeatureReference(featureTableName, "trip_cost");
    FeatureReference feature2Reference =
        DataGenerator.createFeatureReference(featureTableName, "trip_distance");

    Value entityValue = Value.newBuilder().setInt64Val(1).build();
    byte[] redisKey =
        DigestUtils.md5Hex(
                String.format(
                    "%s#%s:%s",
                    FEAST_PROJECT, "driver", Long.valueOf(entityValue.getInt64Val()).toString()))
            .getBytes();

    ImmutableMap<FeatureReference, Value> featureReferenceValueMap =
        ImmutableMap.of(
            feature1Reference,
            DataGenerator.createInt64Value(5L),
            feature2Reference,
            DataGenerator.createDoubleValue(3.5));

    String eventTimestampKey = "_ts:" + featureTableName;
    Timestamp eventTimestampValue = Timestamp.newBuilder().setSeconds(100).build();

    // Insert timestamp into Redis and isTimestampMap only once
    byte[] timestampBytes =
        Hashing.murmur3_32().hashString(eventTimestampKey, StandardCharsets.UTF_8).asBytes();
    syncCommands.hset(redisKey, timestampBytes, eventTimestampValue.toByteArray());
    featureReferenceValueMap.forEach(
        (featureReference, featureValue) -> {
          // Murmur hash Redis Feature Field i.e murmur(<rides:trip_distance>)
          String delimitedFeatureReference =
              featureReference.getFeatureTable() + ":" + featureReference.getName();
          byte[] featureReferenceBytes =
              Hashing.murmur3_32()
                  .hashString(delimitedFeatureReference, StandardCharsets.UTF_8)
                  .asBytes();
          // Insert features into Redis
          syncCommands.hset(redisKey, featureReferenceBytes, featureValue.toByteArray());
        });
  }

  @Test
  public void shouldRetrieveFeaturesSuccessfully() {
    RedisOnlineRetriever retriever =
        new RedisOnlineRetriever(new RedisClient(syncCommands.getStatefulConnection()));
    List<FeatureReference> featureReferences =
        Stream.of("trip_cost", "trip_distance")
            .map(f -> FeatureReference.newBuilder().setFeatureTable("rides").setName(f).build())
            .toList();
    List<String> entityNames = List.of("driver");
    List<ServingServiceProto.GetOnlineFeaturesRequest.EntityRow> entityRows =
        List.of(DataGenerator.createEntityRow("driver", DataGenerator.createInt64Value(1), 100));
    List<List<Feature>> featuresForRows =
        retriever.getOnlineFeatures(FEAST_PROJECT, entityRows, featureReferences, entityNames);
    assertEquals(1, featuresForRows.size());
    List<Feature> features = featuresForRows.get(0);
    assertEquals(2, features.size());
    assertEquals(
        5L, features.get(0).getFeatureValue(ValueProto.ValueType.Enum.INT64).getInt64Val());
    assertEquals(featureReferences.get(0), features.get(0).getFeatureReference());
    assertEquals(
        3.5, features.get(1).getFeatureValue(ValueProto.ValueType.Enum.DOUBLE).getDoubleVal());
    assertEquals(featureReferences.get(1), features.get(1).getFeatureReference());
  }
}
