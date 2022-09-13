package dev.caraml.serving.store;

import static dev.caraml.store.testutils.it.DataGenerator.createEmptyValue;
import static dev.caraml.store.testutils.it.DataGenerator.createInt64Value;
import static dev.caraml.store.testutils.it.DataGenerator.createStrValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import dev.caraml.serving.featurespec.FeatureSpecService;
import dev.caraml.store.protobuf.core.FeatureProto.FeatureSpec;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FieldList;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FieldStatus;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesResponse;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesResponseMetadata;
import dev.caraml.store.protobuf.types.ValueProto.ValueType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StoreServiceTest {
  @Mock FeatureSpecService specService;
  @Mock OnlineRetriever retriever;

  private StoreService storeService;

  List<Feature> mockedFeatureRows;
  List<FeatureSpec> featureSpecs;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    storeService = new StoreService(retriever, specService);

    mockedFeatureRows = new ArrayList<>();
    mockedFeatureRows.add(
        new ProtoFeature(
            FeatureReference.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_1")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("1")));
    mockedFeatureRows.add(
        new ProtoFeature(
            FeatureReference.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_2")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("2")));
    mockedFeatureRows.add(
        new ProtoFeature(
            FeatureReference.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_1")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("3")));
    mockedFeatureRows.add(
        new ProtoFeature(
            FeatureReference.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_2")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("4")));
    mockedFeatureRows.add(
        new ProtoFeature(
            FeatureReference.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_3")
                .build(),
            Timestamp.newBuilder().setSeconds(100).build(),
            createStrValue("5")));
    mockedFeatureRows.add(
        new ProtoFeature(
            FeatureReference.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_1")
                .build(),
            Timestamp.newBuilder().setSeconds(50).build(),
            createStrValue("6")));

    featureSpecs = new ArrayList<>();
    featureSpecs.add(
        FeatureSpec.newBuilder().setName("feature_1").setValueType(ValueType.Enum.STRING).build());
    featureSpecs.add(
        FeatureSpec.newBuilder().setName("feature_2").setValueType(ValueType.Enum.STRING).build());
  }

  @Test
  public void shouldReturnResponseWithValuesAndMetadataIfKeysPresent() {
    String projectName = "default";
    FeatureReference featureReference1 =
        FeatureReference.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_1")
            .build();
    FeatureReference featureReference2 =
        FeatureReference.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_2")
            .build();
    List<FeatureReference> featureReferences = List.of(featureReference1, featureReference2);
    GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(projectName, featureReferences);

    List<Feature> entityKeyList1 = new ArrayList<>();
    List<Feature> entityKeyList2 = new ArrayList<>();
    entityKeyList1.add(mockedFeatureRows.get(0));
    entityKeyList1.add(mockedFeatureRows.get(1));
    entityKeyList2.add(mockedFeatureRows.get(2));
    entityKeyList2.add(mockedFeatureRows.get(3));

    List<List<Feature>> featureRows = List.of(entityKeyList1, entityKeyList2);

    when(retriever.getOnlineFeatures(any(), any(), any(), any())).thenReturn(featureRows);
    when(specService.getFeatureTableSpec(any(), any())).thenReturn(getFeatureTableSpec());
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(0).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(2).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(3).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));

    FieldList fieldList =
        FieldList.newBuilder()
            .addAllVal(
                Arrays.asList(
                    "entity1", "entity2", "featuretable_1:feature_1", "featuretable_1:feature_2"))
            .build();

    GetOnlineFeaturesResponse expectedResponse =
        GetOnlineFeaturesResponse.newBuilder()
            .setMetadata(
                GetOnlineFeaturesResponseMetadata.newBuilder().setFieldNames(fieldList).build())
            .addResults(
                GetOnlineFeaturesResponse.FieldVector.newBuilder()
                    .addAllValues(
                        Arrays.asList(
                            createInt64Value(1),
                            createStrValue("a"),
                            createStrValue("1"),
                            createStrValue("2")))
                    .addAllStatuses(
                        Arrays.asList(
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT))
                    .build())
            .addResults(
                GetOnlineFeaturesResponse.FieldVector.newBuilder()
                    .addAllValues(
                        Arrays.asList(
                            createInt64Value(2),
                            createStrValue("b"),
                            createStrValue("3"),
                            createStrValue("4")))
                    .addAllStatuses(
                        Arrays.asList(
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT))
                    .build())
            .build();

    GetOnlineFeaturesResponse actualResponse = storeService.getOnlineFeatures(request);
    assertThat(actualResponse, equalTo(expectedResponse));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesAndMetadataIfKeysNotPresent() {
    String projectName = "default";
    FeatureReference featureReference1 =
        FeatureReference.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_1")
            .build();
    FeatureReference featureReference2 =
        FeatureReference.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_2")
            .build();
    List<FeatureReference> featureReferences = List.of(featureReference1, featureReference2);
    GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(projectName, featureReferences);

    List<Feature> entityKeyList1 = new ArrayList<>();
    List<Feature> entityKeyList2 = new ArrayList<>();
    entityKeyList1.add(mockedFeatureRows.get(0));
    entityKeyList1.add(mockedFeatureRows.get(1));
    entityKeyList2.add(mockedFeatureRows.get(4));

    List<List<Feature>> featureRows = List.of(entityKeyList1, entityKeyList2);

    when(retriever.getOnlineFeatures(any(), any(), any(), any())).thenReturn(featureRows);
    when(specService.getFeatureTableSpec(any(), any())).thenReturn(getFeatureTableSpec());
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(0).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));

    FieldList fieldList =
        FieldList.newBuilder()
            .addAllVal(
                Arrays.asList(
                    "entity1", "entity2", "featuretable_1:feature_1", "featuretable_1:feature_2"))
            .build();

    GetOnlineFeaturesResponse expectedResponse =
        GetOnlineFeaturesResponse.newBuilder()
            .setMetadata(
                GetOnlineFeaturesResponseMetadata.newBuilder().setFieldNames(fieldList).build())
            .addResults(
                GetOnlineFeaturesResponse.FieldVector.newBuilder()
                    .addAllValues(
                        Arrays.asList(
                            createInt64Value(1),
                            createStrValue("a"),
                            createStrValue("1"),
                            createStrValue("2")))
                    .addAllStatuses(
                        Arrays.asList(
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT))
                    .build())
            .addResults(
                GetOnlineFeaturesResponse.FieldVector.newBuilder()
                    .addAllValues(
                        Arrays.asList(
                            createInt64Value(2),
                            createStrValue("b"),
                            createEmptyValue(),
                            createEmptyValue()))
                    .addAllStatuses(
                        Arrays.asList(
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT,
                            FieldStatus.NOT_FOUND,
                            FieldStatus.NOT_FOUND))
                    .build())
            .build();

    GetOnlineFeaturesResponse actualResponse = storeService.getOnlineFeatures(request);
    assertThat(actualResponse, equalTo(expectedResponse));
  }

  @Test
  public void shouldReturnResponseWithUnsetValuesAndMetadataIfMaxAgeIsExceeded() {
    String projectName = "default";
    FeatureReference featureReference1 =
        FeatureReference.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_1")
            .build();
    FeatureReference featureReference2 =
        FeatureReference.newBuilder()
            .setFeatureTable("featuretable_1")
            .setName("feature_2")
            .build();
    List<FeatureReference> featureReferences = List.of(featureReference1, featureReference2);
    GetOnlineFeaturesRequest request = getOnlineFeaturesRequest(projectName, featureReferences);

    List<Feature> entityKeyList1 = new ArrayList<>();
    List<Feature> entityKeyList2 = new ArrayList<>();
    entityKeyList1.add(mockedFeatureRows.get(5));
    entityKeyList1.add(mockedFeatureRows.get(1));
    entityKeyList2.add(mockedFeatureRows.get(5));
    entityKeyList2.add(mockedFeatureRows.get(1));

    List<List<Feature>> featureRows = List.of(entityKeyList1, entityKeyList2);

    when(retriever.getOnlineFeatures(any(), any(), any(), any())).thenReturn(featureRows);
    when(specService.getFeatureTableSpec(any(), any()))
        .thenReturn(
            FeatureTableSpec.newBuilder()
                .setName("featuretable_1")
                .addEntities("entity1")
                .addEntities("entity2")
                .addFeatures(
                    FeatureSpec.newBuilder()
                        .setName("feature_1")
                        .setValueType(ValueType.Enum.STRING)
                        .build())
                .addFeatures(
                    FeatureSpec.newBuilder()
                        .setName("feature_2")
                        .setValueType(ValueType.Enum.STRING)
                        .build())
                .setMaxAge(Duration.newBuilder().setSeconds(1))
                .build());
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(1).getFeatureReference()))
        .thenReturn(featureSpecs.get(1));
    when(specService.getFeatureSpec(projectName, mockedFeatureRows.get(5).getFeatureReference()))
        .thenReturn(featureSpecs.get(0));

    FieldList fieldList =
        FieldList.newBuilder()
            .addAllVal(
                Arrays.asList(
                    "entity1", "entity2", "featuretable_1:feature_1", "featuretable_1:feature_2"))
            .build();

    GetOnlineFeaturesResponse expectedResponse =
        GetOnlineFeaturesResponse.newBuilder()
            .setMetadata(
                GetOnlineFeaturesResponseMetadata.newBuilder().setFieldNames(fieldList).build())
            .addResults(
                GetOnlineFeaturesResponse.FieldVector.newBuilder()
                    .addAllValues(
                        Arrays.asList(
                            createInt64Value(1),
                            createStrValue("a"),
                            createEmptyValue(),
                            createStrValue("2")))
                    .addAllStatuses(
                        Arrays.asList(
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT,
                            FieldStatus.OUTSIDE_MAX_AGE,
                            FieldStatus.PRESENT))
                    .build())
            .addResults(
                GetOnlineFeaturesResponse.FieldVector.newBuilder()
                    .addAllValues(
                        Arrays.asList(
                            createInt64Value(2),
                            createStrValue("b"),
                            createEmptyValue(),
                            createStrValue("2")))
                    .addAllStatuses(
                        Arrays.asList(
                            FieldStatus.PRESENT,
                            FieldStatus.PRESENT,
                            FieldStatus.OUTSIDE_MAX_AGE,
                            FieldStatus.PRESENT))
                    .build())
            .build();

    GetOnlineFeaturesResponse actualResponse = storeService.getOnlineFeatures(request);
    assertThat(actualResponse, equalTo(expectedResponse));
  }

  private FeatureTableSpec getFeatureTableSpec() {
    return FeatureTableSpec.newBuilder()
        .setName("featuretable_1")
        .addEntities("entity1")
        .addEntities("entity2")
        .addFeatures(
            FeatureSpec.newBuilder()
                .setName("feature_1")
                .setValueType(ValueType.Enum.STRING)
                .build())
        .addFeatures(
            FeatureSpec.newBuilder()
                .setName("feature_2")
                .setValueType(ValueType.Enum.STRING)
                .build())
        .setMaxAge(Duration.newBuilder().setSeconds(120))
        .build();
  }

  private GetOnlineFeaturesRequest getOnlineFeaturesRequest(
      String projectName, List<FeatureReference> featureReferences) {
    return GetOnlineFeaturesRequest.newBuilder()
        .setProject(projectName)
        .addAllFeatures(featureReferences)
        .addEntityRows(
            GetOnlineFeaturesRequest.EntityRow.newBuilder()
                .setTimestamp(Timestamp.newBuilder().setSeconds(100))
                .putFields("entity1", createInt64Value(1))
                .putFields("entity2", createStrValue("a")))
        .addEntityRows(
            GetOnlineFeaturesRequest.EntityRow.newBuilder()
                .setTimestamp(Timestamp.newBuilder().setSeconds(100))
                .putFields("entity1", createInt64Value(2))
                .putFields("entity2", createStrValue("b")))
        .addFeatures(
            FeatureReference.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_1")
                .build())
        .addFeatures(
            FeatureReference.newBuilder()
                .setFeatureTable("featuretable_1")
                .setName("feature_2")
                .build())
        .build();
  }
}
