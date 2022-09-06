package dev.caraml.serving.featurespec;

import static org.grpcmock.GrpcMock.getGlobalPort;
import static org.grpcmock.GrpcMock.grpcMock;
import static org.grpcmock.GrpcMock.stubFor;
import static org.grpcmock.GrpcMock.unaryMethod;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dev.caraml.store.protobuf.core.CoreServiceGrpc;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListFeatureTablesResponse;
import dev.caraml.store.protobuf.core.CoreServiceProto.ListProjectsResponse;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTable;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.types.ValueProto.ValueType;
import dev.caraml.store.testutils.it.DataGenerator;
import java.util.List;
import org.grpcmock.GrpcMock;
import org.grpcmock.junit5.GrpcMockExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockitoAnnotations;

@ExtendWith(GrpcMockExtension.class)
public class FeatureSpecServiceTest {

  private CacheProperties cacheProperties = new CacheProperties(120, 60);
  private ImmutableList<String> featureTableEntities;
  private ImmutableMap<String, ValueType.Enum> featureTable1Features;
  private ImmutableMap<String, ValueType.Enum> featureTable2Features;
  private FeatureTableSpec featureTable1Spec;
  private FeatureTableSpec featureTable2Spec;

  @BeforeAll
  static void createServer() {
    GrpcMock.configureFor(grpcMock().build().start());
  }

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    setupProject("default");
    featureTableEntities = ImmutableList.of("entity1");
    featureTable1Features =
        ImmutableMap.of(
            "trip_cost1", ValueType.Enum.INT64,
            "trip_distance1", ValueType.Enum.DOUBLE,
            "trip_empty1", ValueType.Enum.DOUBLE);
    featureTable2Features =
        ImmutableMap.of(
            "trip_cost2", ValueType.Enum.INT64,
            "trip_distance2", ValueType.Enum.DOUBLE,
            "trip_empty2", ValueType.Enum.DOUBLE);
    featureTable1Spec =
        DataGenerator.createFeatureTableSpec(
            "featuretable1", featureTableEntities, featureTable1Features, 7200, ImmutableMap.of());
    featureTable2Spec =
        DataGenerator.createFeatureTableSpec(
            "featuretable2", featureTableEntities, featureTable2Features, 7200, ImmutableMap.of());

    setupFeatureTableAndProject("default", List.of(featureTable1Spec, featureTable2Spec));
  }

  private void setupProject(String project) {
    stubFor(
        unaryMethod(CoreServiceGrpc.getListProjectsMethod())
            .willReturn(ListProjectsResponse.newBuilder().addProjects(project).build()));
  }

  private void setupFeatureTableAndProject(String project, List<FeatureTableSpec> specs) {
    List<FeatureTable> tables =
        specs.stream().map(sp -> FeatureTable.newBuilder().setSpec(sp).build()).toList();
    stubFor(
        unaryMethod(CoreServiceGrpc.getListFeatureTablesMethod())
            .willReturn(ListFeatureTablesResponse.newBuilder().addAllTables(tables).build()));
  }

  @Test
  public void shouldPopulateAndReturnDifferentFeatureTables() {
    FeatureSpecServiceConfig config = new FeatureSpecServiceConfig();
    config.setCache(cacheProperties);
    config.setHost("localhost");
    config.setPort(getGlobalPort());
    FeatureSpecService service = new FeatureSpecService(config);
    service.populateCache();
    FeatureReference featureReference1 =
        FeatureReference.newBuilder()
            .setFeatureTable("featuretable1")
            .setName("trip_cost1")
            .build();
    FeatureReference featureReference2 =
        FeatureReference.newBuilder()
            .setFeatureTable("featuretable1")
            .setName("trip_distance1")
            .build();
    FeatureReference featureReference3 =
        FeatureReference.newBuilder()
            .setFeatureTable("featuretable2")
            .setName("trip_empty2")
            .build();
    assertEquals(featureTable1Spec, service.getFeatureTableSpec("default", featureReference1));
    assertEquals(featureTable1Spec, service.getFeatureTableSpec("default", featureReference2));
    assertEquals(featureTable2Spec, service.getFeatureTableSpec("default", featureReference3));
  }
}
