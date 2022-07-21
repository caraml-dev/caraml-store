package dev.caraml.store.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.Duration;
import dev.caraml.store.protobuf.core.*;
import dev.caraml.store.protobuf.core.OnlineStoreProto;
import dev.caraml.store.protobuf.types.ValueProto;
import dev.caraml.store.testutils.it.DataGenerator;
import dev.caraml.store.testutils.it.SimpleCoreClient;
import dev.caraml.store.testutils.util.TestUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.*;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class SpecServiceIT extends BaseIT {

  static CoreServiceGrpc.CoreServiceBlockingStub stub;
  static SimpleCoreClient apiClient;

  @BeforeAll
  public static void globalSetUp(@Value("${grpc.server.port}") int port) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    stub = CoreServiceGrpc.newBlockingStub(channel);
    apiClient = new SimpleCoreClient(stub);
  }

  private FeatureTableProto.FeatureTableSpec example1;
  private FeatureTableProto.FeatureTableSpec example2;

  @BeforeEach
  public void initState() {

    EntityProto.EntitySpec entitySpec1 =
        DataGenerator.createEntitySpec(
            "entity1",
            "Entity 1 description",
            ValueProto.ValueType.Enum.STRING,
            Map.of("label_key", "label_value"));
    EntityProto.EntitySpec entitySpec2 =
        DataGenerator.createEntitySpec(
            "entity2",
            "Entity 2 description",
            ValueProto.ValueType.Enum.STRING,
            Map.of("label_key2", "label_value2"));
    apiClient.simpleApplyEntity("default", entitySpec1);
    apiClient.simpleApplyEntity("default", entitySpec2);

    example1 =
        DataGenerator.createFeatureTableSpec(
                "featuretable1",
                Arrays.asList("entity1", "entity2"),
                new HashMap<>() {
                  {
                    put("feature1", ValueProto.ValueType.Enum.STRING);
                    put("feature2", ValueProto.ValueType.Enum.FLOAT);
                  }
                },
                7200,
                Map.of("feat_key2", "feat_value2"))
            .toBuilder()
            .setBatchSource(
                DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""))
            .setOnlineStore(DataGenerator.createOnlineStore("default"))
            .build();

    example2 =
        DataGenerator.createFeatureTableSpec(
                "featuretable2",
                Arrays.asList("entity1", "entity2"),
                new HashMap<>() {
                  {
                    put("feature3", ValueProto.ValueType.Enum.STRING);
                    put("feature4", ValueProto.ValueType.Enum.FLOAT);
                  }
                },
                7200,
                Map.of("feat_key4", "feat_value4"))
            .toBuilder()
            .setBatchSource(
                DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""))
            .build();

    apiClient.registerOnlineStore(DataGenerator.createOnlineStore("unset"));
    apiClient.registerOnlineStore(DataGenerator.createOnlineStore("default"));
    apiClient.applyFeatureTable("default", example1);
    apiClient.applyFeatureTable("default", example2);
    apiClient.simpleApplyEntity(
        "project1",
        DataGenerator.createEntitySpec(
            "entity3",
            "Entity 3 description",
            ValueProto.ValueType.Enum.STRING,
            Map.of("label_key2", "label_value2")));
  }

  @Nested
  class ListEntities {
    @Test
    public void shouldFilterEntitiesByLabels() {
      List<EntityProto.Entity> entities =
          apiClient.simpleListEntities("", Map.of("label_key2", "label_value2"));
      assertThat(entities, hasSize(1));
      assertThat(entities, hasItem(hasProperty("spec", hasProperty("name", equalTo("entity2")))));
    }

    @Test
    public void shouldUseDefaultProjectIfProjectUnspecified() {
      List<EntityProto.Entity> entities = apiClient.simpleListEntities("");

      assertThat(entities, hasSize(2));
      assertThat(entities, hasItem(hasProperty("spec", hasProperty("name", equalTo("entity1")))));
    }

    @Test
    public void shouldFilterEntitiesByProjectAndLabels() {
      List<EntityProto.Entity> entities =
          apiClient.simpleListEntities("project1", Map.of("label_key2", "label_value2"));
      assertThat(entities, hasSize(1));
      assertThat(entities, hasItem(hasProperty("spec", hasProperty("name", equalTo("entity3")))));
    }
  }

  @Nested
  class ListFeatureTables {
    @Test
    public void shouldFilterFeatureTablesByProjectAndLabels() {
      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default")
              .putAllLabels(Map.of("feat_key2", "feat_value2"))
              .build();
      List<FeatureTableProto.FeatureTable> featureTables =
          apiClient.simpleListFeatureTables(filter);

      assertThat(featureTables, hasSize(1));
      assertThat(
          featureTables,
          hasItem(hasProperty("spec", hasProperty("name", equalTo("featuretable1")))));
    }

    @Test
    public void shouldUseDefaultProjectIfProjectUnspecified() {
      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default")
              .build();
      List<FeatureTableProto.FeatureTable> featureTables =
          apiClient.simpleListFeatureTables(filter);

      assertThat(featureTables, hasSize(2));
      assertThat(
          featureTables,
          hasItem(hasProperty("spec", hasProperty("name", equalTo("featuretable1")))));
      assertThat(
          featureTables,
          hasItem(hasProperty("spec", hasProperty("name", equalTo("featuretable2")))));
    }
  }

  @Nested
  class ApplyEntity {
    @Test
    public void shouldThrowExceptionGivenEntityWithDash() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.simpleApplyEntity(
                      "default",
                      DataGenerator.createEntitySpec(
                          "dash-entity",
                          "Dash Entity description",
                          ValueProto.ValueType.Enum.STRING,
                          Map.of("test_key", "test_value"))));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INTERNAL: invalid value for %s resource, %s: %s",
                  "entity",
                  "dash-entity",
                  "argument must only contain alphanumeric characters and underscores.")));
    }

    @Test
    public void shouldThrowExceptionIfTypeChanged() {
      String projectName = "default";

      EntityProto.EntitySpec spec =
          DataGenerator.createEntitySpec(
              "entity1",
              "Entity description",
              ValueProto.ValueType.Enum.FLOAT,
              Map.of("label_key", "label_value"));

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.simpleApplyEntity("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INTERNAL: You are attempting to change the type of this entity in %s project from %s to %s. This isn't allowed. Please create a new entity.",
                  "default", "STRING", spec.getValueType())));
    }

    @Test
    public void shouldReturnEntityIfEntityHasNotChanged() {
      String projectName = "default";
      EntityProto.EntitySpec spec = apiClient.simpleGetEntity(projectName, "entity1").getSpec();

      CoreServiceProto.ApplyEntityResponse response =
          apiClient.simpleApplyEntity(projectName, spec);

      assertThat(response.getEntity().getSpec().getName(), equalTo(spec.getName()));
      assertThat(response.getEntity().getSpec().getDescription(), equalTo(spec.getDescription()));
      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
      assertThat(response.getEntity().getSpec().getValueType(), equalTo(spec.getValueType()));
    }

    @Test
    public void shouldApplyEntityIfNotExists() {
      String projectName = "default";
      EntityProto.EntitySpec spec =
          DataGenerator.createEntitySpec(
              "new_entity",
              "Entity description",
              ValueProto.ValueType.Enum.STRING,
              Map.of("label_key", "label_value"));

      CoreServiceProto.ApplyEntityResponse response =
          apiClient.simpleApplyEntity(projectName, spec);

      assertThat(response.getEntity().getSpec().getName(), equalTo(spec.getName()));
      assertThat(response.getEntity().getSpec().getDescription(), equalTo(spec.getDescription()));
      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
      assertThat(response.getEntity().getSpec().getValueType(), equalTo(spec.getValueType()));
    }

    @Test
    public void shouldCreateProjectWhenNotAlreadyExists() {
      EntityProto.EntitySpec spec =
          DataGenerator.createEntitySpec(
              "new_entity2",
              "Entity description",
              ValueProto.ValueType.Enum.STRING,
              Map.of("key1", "val1"));
      CoreServiceProto.ApplyEntityResponse response =
          apiClient.simpleApplyEntity("new_project", spec);

      assertThat(response.getEntity().getSpec().getName(), equalTo(spec.getName()));
      assertThat(response.getEntity().getSpec().getDescription(), equalTo(spec.getDescription()));
      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
      assertThat(response.getEntity().getSpec().getValueType(), equalTo(spec.getValueType()));
    }

    @Test
    public void shouldFailWhenProjectIsArchived() {
      apiClient.createProject("archived");
      apiClient.archiveProject("archived");

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.simpleApplyEntity(
                      "archived",
                      DataGenerator.createEntitySpec(
                          "new_entity3",
                          "Entity description",
                          ValueProto.ValueType.Enum.STRING,
                          Map.of("key1", "val1"))));
      assertThat(exc.getMessage(), equalTo("INTERNAL: Project is archived: archived"));
    }

    @Test
    public void shouldUpdateLabels() {
      EntityProto.EntitySpec spec =
          DataGenerator.createEntitySpec(
              "entity1",
              "Entity description",
              ValueProto.ValueType.Enum.STRING,
              Map.of("label_key", "label_value", "label_key2", "label_value2"));

      CoreServiceProto.ApplyEntityResponse response = apiClient.simpleApplyEntity("default", spec);

      assertThat(response.getEntity().getSpec().getLabelsMap(), equalTo(spec.getLabelsMap()));
    }
  }

  @Nested
  class GetEntity {
    @Test
    public void shouldThrowExceptionGivenMissingEntity() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.simpleGetEntity("default", ""));

      assertThat(exc.getMessage(), equalTo("INVALID_ARGUMENT: No entity name provided"));
    }

    public void shouldRetrieveFromDefaultIfProjectNotSpecified() {
      String entityName = "entity1";
      EntityProto.Entity entity = apiClient.simpleGetEntity("", entityName);

      assertThat(entity.getSpec().getName(), equalTo(entityName));
    }
  }

  @Nested
  class GetFeatureTable {
    @Test
    public void shouldThrowExceptionGivenNoSuchFeatureTable() {
      String projectName = "default";
      String featureTableName = "invalid_table";
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () -> apiClient.simpleGetFeatureTable(projectName, featureTableName));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "NOT_FOUND: No such Feature Table: (project: %s, name: %s)",
                  projectName, featureTableName)));
    }

    @Test
    public void shouldReturnFeatureTableIfExists() {
      FeatureTableProto.FeatureTable featureTable =
          apiClient.simpleGetFeatureTable("default", "featuretable1");

      assertTrue(TestUtil.compareFeatureTableSpec(featureTable.getSpec(), example1));
    }
  }

  @Nested
  class ListFeatures {
    @Test
    public void shouldFilterFeaturesByEntitiesAndLabels() {
      // Case 1: Only filter by entities
      Map<String, FeatureProto.FeatureSpec> result1 =
          apiClient.simpleListFeatures("default", "entity1", "entity2");

      assertThat(result1, aMapWithSize(4));
      assertThat(result1, hasKey(equalTo("featuretable1:feature1")));
      assertThat(result1, hasKey(equalTo("featuretable1:feature2")));
      assertThat(result1, hasKey(equalTo("featuretable2:feature3")));
      assertThat(result1, hasKey(equalTo("featuretable2:feature4")));

      // Case 2: Filter by entities and labels
      Map<String, FeatureProto.FeatureSpec> result2 =
          apiClient.simpleListFeatures(
              "default", Map.of("feat_key2", "feat_value2"), List.of("entity1", "entity2"));

      assertThat(result2, aMapWithSize(2));
      assertThat(result2, hasKey(equalTo("featuretable1:feature1")));
      assertThat(result2, hasKey(equalTo("featuretable1:feature2")));

      // Case 3: Filter by labels
      Map<String, FeatureProto.FeatureSpec> result3 =
          apiClient.simpleListFeatures(
              "default", Map.of("feat_key4", "feat_value4"), Collections.emptyList());

      assertThat(result3, aMapWithSize(2));
      assertThat(result3, hasKey(equalTo("featuretable2:feature3")));
      assertThat(result3, hasKey(equalTo("featuretable2:feature4")));

      // Case 4: Filter by nothing, except project
      Map<String, FeatureProto.FeatureSpec> result4 =
          apiClient.simpleListFeatures("project1", Map.of(), Collections.emptyList());

      assertThat(result4, aMapWithSize(0));

      // Case 5: Filter by nothing; will use default project
      Map<String, FeatureProto.FeatureSpec> result5 =
          apiClient.simpleListFeatures("", Collections.emptyMap(), Collections.emptyList());

      assertThat(result5, aMapWithSize(4));
      assertThat(result5, hasKey(equalTo("featuretable1:feature1")));
      assertThat(result5, hasKey(equalTo("featuretable1:feature2")));
      assertThat(result5, hasKey(equalTo("featuretable2:feature3")));
      assertThat(result5, hasKey(equalTo("featuretable2:feature4")));

      // Case 6: Filter by mismatched entity
      Map<String, FeatureProto.FeatureSpec> result6 =
          apiClient.simpleListFeatures("default", Collections.emptyMap(), List.of("entity1"));
      assertThat(result6, aMapWithSize(0));
    }
  }

  @Nested
  public class ApplyFeatureTable {
    private FeatureTableProto.FeatureTableSpec getTestSpec() {
      return example1.toBuilder()
          .setName("apply_test")
          .setOnlineStore(DataGenerator.createOnlineStore("default"))
          .setStreamSource(
              DataGenerator.createKafkaDataSourceSpec(
                  "localhost:9092", "topic", "class.path", "ts_col"))
          .build();
    }

    @Test
    public void shouldApplyNewValidTable() {
      FeatureTableProto.FeatureTable table = apiClient.applyFeatureTable("default", getTestSpec());

      assertTrue(TestUtil.compareFeatureTableSpec(table.getSpec(), getTestSpec()));
      assertThat(table.getMeta().getRevision(), equalTo(0L));
    }

    @Test
    public void shouldUpdateExistingTableWithValidSpec() {
      FeatureTableProto.FeatureTable table = apiClient.applyFeatureTable("default", getTestSpec());

      FeatureTableProto.FeatureTableSpec updatedSpec =
          getTestSpec().toBuilder()
              .clearFeatures()
              .addFeatures(
                  DataGenerator.createFeatureSpec(
                      "feature5", ValueProto.ValueType.Enum.FLOAT, Collections.emptyMap()))
              .setStreamSource(
                  DataGenerator.createKafkaDataSourceSpec(
                      "localhost:9092", "new_topic", "new.class", "ts_col"))
              .setOnlineStore(DataGenerator.createOnlineStore("default"))
              .build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
      assertThat(updatedTable.getMeta().getRevision(), equalTo(table.getMeta().getRevision() + 1L));
      assertThat(
          updatedTable.getSpec().getOnlineStore(),
          equalTo(DataGenerator.createOnlineStore("default")));
    }

    @Test
    public void shouldUpdateFeatureTableOnEntityChange() {
      FeatureTableProto.FeatureTableSpec updatedSpec =
          getTestSpec().toBuilder().clearEntities().addEntities("entity1").build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
    }

    @Test
    public void shouldUpdateFeatureTableOnMaxAgeChange() {
      FeatureTableProto.FeatureTableSpec updatedSpec =
          getTestSpec().toBuilder()
              .setMaxAge(Duration.newBuilder().setSeconds(600).build())
              .build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
    }

    @Test
    public void shouldUpdateFeatureTableOnFeatureTypeChange() {
      int featureIdx =
          IntStream.range(0, getTestSpec().getFeaturesCount())
              .filter(i -> getTestSpec().getFeatures(i).getName().equals("feature2"))
              .findFirst()
              .orElse(-1);

      FeatureTableProto.FeatureTableSpec updatedSpec =
          getTestSpec().toBuilder()
              .setFeatures(
                  featureIdx,
                  DataGenerator.createFeatureSpec(
                      "feature2", ValueProto.ValueType.Enum.STRING_LIST, Collections.emptyMap()))
              .build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
    }

    @Test
    public void shouldUpdateFeatureTableOnFeatureAddition() {
      FeatureTableProto.FeatureTableSpec updatedSpec =
          getTestSpec().toBuilder()
              .addFeatures(
                  DataGenerator.createFeatureSpec(
                      "feature6", ValueProto.ValueType.Enum.FLOAT, Collections.emptyMap()))
              .build();

      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", updatedSpec);

      assertTrue(TestUtil.compareFeatureTableSpec(updatedTable.getSpec(), updatedSpec));
    }

    @Test
    public void shouldNotUpdateIfNoChanges() {
      FeatureTableProto.FeatureTable table = apiClient.applyFeatureTable("default", getTestSpec());
      FeatureTableProto.FeatureTable updatedTable =
          apiClient.applyFeatureTable("default", getTestSpec());

      assertThat(updatedTable.getMeta().getRevision(), equalTo(table.getMeta().getRevision()));
    }

    @Test
    public void shouldErrorOnMissingBatchSource() {
      FeatureTableProto.FeatureTableSpec spec =
          DataGenerator.createFeatureTableSpec(
                  "ft",
                  List.of("entity1"),
                  Map.of("event_timestamp", ValueProto.ValueType.Enum.INT64),
                  3600,
                  Map.of())
              .toBuilder()
              .clearBatchSource()
              .build();

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.applyFeatureTable("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo("INVALID_ARGUMENT: FeatureTable batch source cannot be empty."));
    }

    @Test
    public void shouldErrorOnInvalidBigQueryTableRef() {
      String invalidTableRef = "invalid.bq:path";
      FeatureTableProto.FeatureTableSpec spec =
          DataGenerator.createFeatureTableSpec(
                  "ft",
                  List.of("entity1"),
                  Map.of("feature", ValueProto.ValueType.Enum.INT64),
                  3600,
                  Map.of())
              .toBuilder()
              .setBatchSource(
                  DataGenerator.createBigQueryDataSourceSpec(invalidTableRef, "ts_col", ""))
              .build();

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class, () -> apiClient.applyFeatureTable("default", spec));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "INVALID_ARGUMENT: invalid value for FeatureTable resource, %s: argument must be in the form of <project:dataset.table> .",
                  invalidTableRef)));
    }

    @Test
    public void shouldErrorOnReservedNames() {
      // Reserved name used as feature name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft",
                          List.of("entity1"),
                          Map.of("event_timestamp", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));

      // Reserved name used in as entity name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft",
                          List.of("created_timestamp"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnInvalidName() {
      // Invalid feature table name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "f-t",
                          List.of("entity1"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));

      // Invalid feature name
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft",
                          List.of("entity1"),
                          Map.of("feature-1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnNotFoundEntityName() {
      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "default",
                  DataGenerator.createFeatureTableSpec(
                          "ft1",
                          List.of("entity_not_found"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnArchivedProject() {
      apiClient.createProject("archived");
      apiClient.archiveProject("archived");

      assertThrows(
          StatusRuntimeException.class,
          () ->
              apiClient.applyFeatureTable(
                  "archived",
                  DataGenerator.createFeatureTableSpec(
                          "ft1",
                          List.of("entity1", "entity2"),
                          Map.of("feature1", ValueProto.ValueType.Enum.INT64),
                          3600,
                          Map.of())
                      .toBuilder()
                      .setBatchSource(
                          DataGenerator.createFileDataSourceSpec(
                              "file:///path/to/file", "ts_col", ""))
                      .build()));
    }

    @Test
    public void shouldErrorOnInvalidOnlineStore() {
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.applyFeatureTable(
                      "default",
                      example1.toBuilder()
                          .setOnlineStore(DataGenerator.createOnlineStore("made-up"))
                          .build()));

      assertThat(exc.getMessage(), equalTo("INVALID_ARGUMENT: Invalid store: made-up"));
    }

    @Test
    public void shouldErrorOnArchivedOnlineStore() {
      String onlineStoreName = "old-store";
      // register the store
      apiClient.registerOnlineStore(DataGenerator.createOnlineStore(onlineStoreName));
      // archive the store
      apiClient.archiveOnlineStore(onlineStoreName);

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () ->
                  apiClient.applyFeatureTable(
                      "default",
                      example1.toBuilder()
                          .setOnlineStore(DataGenerator.createOnlineStore(onlineStoreName))
                          .build()));

      assertThat(
          exc.getMessage(),
          equalTo(String.format("INVALID_ARGUMENT: Invalid store: %s", onlineStoreName)));
    }
  }

  @Nested
  public class DeleteFeatureTable {

    @Test
    public void shouldReturnNoTables() {
      String projectName = "default";
      String featureTableName = "featuretable1";

      apiClient.deleteFeatureTable(projectName, featureTableName);

      CoreServiceProto.ListFeatureTablesRequest.Filter filter =
          CoreServiceProto.ListFeatureTablesRequest.Filter.newBuilder()
              .setProject("default")
              .putLabels("feat_key2", "feat_value2")
              .build();
      List<FeatureTableProto.FeatureTable> featureTables =
          apiClient.simpleListFeatureTables(filter);

      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () -> apiClient.simpleGetFeatureTable(projectName, featureTableName));

      assertThat(featureTables.size(), equalTo(0));
      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "NOT_FOUND: Feature Table has been deleted: (project: %s, name: %s)",
                  projectName, featureTableName)));
    }

    @Test
    public void shouldUpdateDeletedTable() {
      String projectName = "default";
      String featureTableName = "featuretable1";

      apiClient.deleteFeatureTable(projectName, featureTableName);

      FeatureTableProto.FeatureTableSpec featureTableSpec =
          DataGenerator.createFeatureTableSpec(
                  featureTableName,
                  Arrays.asList("entity1", "entity2"),
                  new HashMap<>() {
                    {
                      put("feature3", ValueProto.ValueType.Enum.INT64);
                    }
                  },
                  7200,
                  Map.of("feat_key3", "feat_value3"))
              .toBuilder()
              .setBatchSource(
                  DataGenerator.createFileDataSourceSpec("file:///path/to/file", "ts_col", ""))
              .build();

      apiClient.applyFeatureTable(projectName, featureTableSpec);

      FeatureTableProto.FeatureTable featureTable =
          apiClient.simpleGetFeatureTable(projectName, featureTableName);

      assertTrue(TestUtil.compareFeatureTableSpec(featureTable.getSpec(), featureTableSpec));
    }

    @Test
    public void shouldErrorIfTableNotExist() {
      String projectName = "default";
      String featureTableName = "nonexistent_table";
      StatusRuntimeException exc =
          assertThrows(
              StatusRuntimeException.class,
              () -> apiClient.deleteFeatureTable(projectName, featureTableName));

      assertThat(
          exc.getMessage(),
          equalTo(
              String.format(
                  "NOT_FOUND: No such Feature Table: (project: %s, name: %s)",
                  projectName, featureTableName)));
    }
  }

  @Nested
  class ListOnlineStores {
    @Test
    public void shouldReturnAllOnlineStores() {
      List<OnlineStoreProto.OnlineStore> allStores =
          apiClient.listOnlineStores().getOnlineStoreList();

      assertTrue(allStores.size() >= 2);
      assertThat(allStores, hasItem(hasProperty("name", equalTo("default"))));
      assertThat(allStores, hasItem(hasProperty("name", equalTo("unset"))));
    }

    @Test
    public void shouldNotReturnArchivedStores() {
      String onlineStoreName = "old-store";
      // register the store
      apiClient.registerOnlineStore(DataGenerator.createOnlineStore(onlineStoreName));
      // archive the store
      apiClient.archiveOnlineStore(onlineStoreName);

      List<OnlineStoreProto.OnlineStore> allStores =
          apiClient.listOnlineStores().getOnlineStoreList();

      assertThat(allStores, not(hasItem(hasProperty("name", equalTo("old-store")))));
    }

    @Test
    public void shouldReturnEmptyListIfNoActiveOnlineStore() {
      // Archive all stores
      apiClient.archiveOnlineStore("default");
      apiClient.archiveOnlineStore("unset");

      List<OnlineStoreProto.OnlineStore> allStores =
          apiClient.listOnlineStores().getOnlineStoreList();

      assertThat(allStores, hasSize(0));
    }
  }

  @Nested
  class GetOnlineStore {
    @Test
    public void shouldReturnOnlineStoreIfExists() {
      CoreServiceProto.GetOnlineStoreResponse response = apiClient.getOnlineStore("default");
      assertEquals(response.getOnlineStore(), DataGenerator.createOnlineStore("default"));
      assertEquals(response.getStatus(), CoreServiceProto.GetOnlineStoreResponse.Status.ACTIVE);
    }

    @Test
    public void shouldThrowNoSuchElementExceptionIfOnlineStoreNotFound() {
      StatusRuntimeException exc =
          assertThrows(StatusRuntimeException.class, () -> apiClient.getOnlineStore("made-up"));
      assertThat(
          exc.getMessage(), equalTo("NOT_FOUND: Online store with name 'made-up' not found"));
    }
  }

  @Nested
  class ArchiveOnlineStore {
    @Test
    public void shouldArchiveOnlineStoreIfExists() {
      String onlineStoreName = "old-store";
      // register the store
      apiClient.registerOnlineStore(DataGenerator.createOnlineStore(onlineStoreName));
      // archive the store
      apiClient.archiveOnlineStore(onlineStoreName);

      // get store and check status
      CoreServiceProto.GetOnlineStoreResponse response = apiClient.getOnlineStore(onlineStoreName);
      assertThat(
          response.getStatus(), equalTo(CoreServiceProto.GetOnlineStoreResponse.Status.ARCHIVED));
    }

    @Test
    public void shouldThrowNoSuchElementExceptionIfOnlineStoresNotFound() {
      StatusRuntimeException exc =
          assertThrows(StatusRuntimeException.class, () -> apiClient.archiveOnlineStore("made-up"));

      assertThat(
          exc.getMessage(), equalTo("NOT_FOUND: Online store with name 'made-up' not found"));
    }
  }

  @Nested
  class RegisterOnlineStore {
    @Test
    public void shouldRegisterNewOnlineStore() {
      OnlineStoreProto.OnlineStore onlineStore = DataGenerator.createOnlineStore("test");

      CoreServiceProto.RegisterOnlineStoreResponse response =
          apiClient.registerOnlineStore(onlineStore);

      assertThat(response.getOnlineStore(), equalTo(onlineStore));
      assertThat(
          response.getStatus(),
          equalTo(CoreServiceProto.RegisterOnlineStoreResponse.Status.REGISTERED));
    }

    @Test
    public void shouldUpdateOnlineStoreIfConfigChanges() {
      OnlineStoreProto.OnlineStore onlineStore =
          DataGenerator.createOnlineStore("default").toBuilder()
              .setType(OnlineStoreProto.StoreType.BIGTABLE)
              .build();

      CoreServiceProto.RegisterOnlineStoreResponse response =
          apiClient.registerOnlineStore(onlineStore);

      assertThat(
          response.getStatus(),
          equalTo(CoreServiceProto.RegisterOnlineStoreResponse.Status.UPDATED));
      assertThat(response.getOnlineStore(), equalTo(onlineStore));
      assertThat(response.getOnlineStore().getType(), equalTo(OnlineStoreProto.StoreType.BIGTABLE));
    }

    @Test
    public void shouldDoNothingIfNoChange() {
      OnlineStoreProto.OnlineStore defaultOnlineStore = DataGenerator.createOnlineStore("default");

      CoreServiceProto.RegisterOnlineStoreResponse response =
          apiClient.registerOnlineStore(defaultOnlineStore);

      assertThat(
          response.getStatus(),
          equalTo(CoreServiceProto.RegisterOnlineStoreResponse.Status.NO_CHANGE));
    }
  }
}
