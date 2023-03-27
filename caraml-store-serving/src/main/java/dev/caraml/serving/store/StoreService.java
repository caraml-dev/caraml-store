package dev.caraml.serving.store;

import com.google.protobuf.Duration;
import dev.caraml.serving.featurespec.FeatureSpecService;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FieldList;
import dev.caraml.store.protobuf.serving.ServingServiceProto.FieldStatus;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesResponse;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesResponseMetadata;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesResponseV2;
import dev.caraml.store.protobuf.types.ValueProto.Value;
import dev.caraml.store.protobuf.types.ValueProto.ValueType;
import io.grpc.Status;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StoreService {

  private final OnlineRetriever retriever;
  private final FeatureSpecService specService;

  private class RetrievedOnlineFeatures {
    protected List<GetOnlineFeaturesRequest.EntityRow> entityRows;
    protected List<String> fieldNames;
    protected List<Map<String, Value>> values;
    protected List<Map<String, FieldStatus>> statuses;

    public RetrievedOnlineFeatures(
        List<GetOnlineFeaturesRequest.EntityRow> entityRows,
        List<String> fieldNames,
        List<Map<String, Value>> values,
        List<Map<String, FieldStatus>> statuses) {
      this.entityRows = entityRows;
      this.fieldNames = fieldNames;
      this.values = values;
      this.statuses = statuses;
    }
  }

  @Autowired
  public StoreService(OnlineRetriever retriever, FeatureSpecService specService) {
    this.retriever = retriever;
    this.specService = specService;
  }

  public GetOnlineFeaturesResponseV2 getOnlineFeaturesV2(GetOnlineFeaturesRequest request) {
    if(request.getEntityRowsList().isEmpty()) {
      throw new IllegalArgumentException("Entity rows cannot be empty");
    }

    RetrievedOnlineFeatures retrievedOnlineFeatures = retrieveOnlineFeatures(request);
    // Build response field values from entityValuesMap and entityStatusesMap
    // Response field values should be in the same order as the entityRows provided by the user.
    List<GetOnlineFeaturesResponseV2.FieldValues> fieldValuesList =
        IntStream.range(0, retrievedOnlineFeatures.entityRows.size())
            .mapToObj(
                entityRowIdx ->
                    GetOnlineFeaturesResponseV2.FieldValues.newBuilder()
                        .putAllFields(retrievedOnlineFeatures.values.get(entityRowIdx))
                        .putAllStatuses(retrievedOnlineFeatures.statuses.get(entityRowIdx))
                        .build())
            .collect(Collectors.toList());

    return GetOnlineFeaturesResponseV2.newBuilder().addAllFieldValues(fieldValuesList).build();
  }

  public GetOnlineFeaturesResponse getOnlineFeatures(GetOnlineFeaturesRequest request) {
    if(request.getEntityRowsList().isEmpty()) {
      throw new IllegalArgumentException("Entity rows cannot be empty");
    }

    RetrievedOnlineFeatures retrievedOnlineFeatures = this.retrieveOnlineFeatures(request);

    // Build response field values from entityValuesMap and entityStatusesMap
    // Response field values should be in the same order as the entityRows provided by the user.
    FieldList fieldList =
        FieldList.newBuilder().addAllVal(retrievedOnlineFeatures.fieldNames).build();

    List<GetOnlineFeaturesResponse.FieldVector> fieldVectorList =
        IntStream.range(0, retrievedOnlineFeatures.entityRows.size())
            .mapToObj(
                entityRowIdx ->
                    GetOnlineFeaturesResponse.FieldVector.newBuilder()
                        // Sort maps by keys using TreeMap before extracting
                        // values and statuses to vector lists
                        .addAllValues(
                            new TreeMap<>(retrievedOnlineFeatures.values.get(entityRowIdx))
                                .values())
                        .addAllStatuses(
                            new TreeMap<>(retrievedOnlineFeatures.statuses.get(entityRowIdx))
                                .values())
                        .build())
            .collect(Collectors.toList());

    return GetOnlineFeaturesResponse.newBuilder()
        .setMetadata(
            GetOnlineFeaturesResponseMetadata.newBuilder().setFieldNames(fieldList).build())
        .addAllResults(fieldVectorList)
        .build();
  }

  private RetrievedOnlineFeatures retrieveOnlineFeatures(GetOnlineFeaturesRequest request) {

    String projectName = request.getProject();
    List<FeatureReference> featureReferences = request.getFeaturesList();

    // Autofill default project if project is not specified
    if (projectName.isEmpty()) {
      projectName = "default";
    }

    List<GetOnlineFeaturesRequest.EntityRow> entityRows = request.getEntityRowsList();
    List<Map<String, Value>> values =
        entityRows.stream().map(r -> new HashMap<>(r.getFieldsMap())).collect(Collectors.toList());
    List<Map<String, FieldStatus>> statuses =
        entityRows.stream()
            .map(
                r ->
                    r.getFieldsMap().entrySet().stream()
                        .map(entry -> Pair.of(entry.getKey(), getMetadata(entry.getValue(), false)))
                        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight)))
            .collect(Collectors.toList());

    String finalProjectName = projectName;
    Map<FeatureReference, Duration> featureMaxAges =
        featureReferences.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    ref -> specService.getFeatureTableSpec(finalProjectName, ref).getMaxAge()));
    List<String> entityNames =
        featureReferences.stream()
            .map(ref -> specService.getFeatureTableSpec(finalProjectName, ref).getEntitiesList())
            .findFirst()
            .get();

    Map<FeatureReference, ValueType.Enum> featureValueTypes =
        featureReferences.stream()
            .distinct()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    ref -> specService.getFeatureSpec(finalProjectName, ref).getValueType()));

    List<List<Feature>> entityRowsFeatures =
        retriever.getOnlineFeatures(projectName, entityRows, featureReferences, entityNames);

    if (entityRowsFeatures.size() != entityRows.size()) {
      throw Status.INTERNAL
          .withDescription(
              "The no. of FeatureRow obtained from OnlineRetriever"
                  + "does not match no. of entityRow passed.")
          .asRuntimeException();
    }

    for (int i = 0; i < entityRows.size(); i++) {
      GetOnlineFeaturesRequest.EntityRow entityRow = entityRows.get(i);
      List<Feature> curEntityRowFeatures = entityRowsFeatures.get(i);

      Map<FeatureReference, Feature> featureReferenceFeatureMap =
          getFeatureRefFeatureMap(curEntityRowFeatures);

      Map<String, Value> rowValues = values.get(i);
      Map<String, FieldStatus> rowStatuses = statuses.get(i);

      for (FeatureReference featureReference : featureReferences) {
        if (featureReferenceFeatureMap.containsKey(featureReference)) {
          Feature feature = featureReferenceFeatureMap.get(featureReference);

          Value value =
              feature.getFeatureValue(featureValueTypes.get(feature.getFeatureReference()));

          Boolean isOutsideMaxAge =
              checkOutsideMaxAge(
                  feature, entityRow, featureMaxAges.get(feature.getFeatureReference()));

          if (!isOutsideMaxAge && value != null) {
            rowValues.put(getFeatureStringRef(feature.getFeatureReference()), value);
          } else {
            rowValues.put(
                getFeatureStringRef(feature.getFeatureReference()), Value.newBuilder().build());
          }

          rowStatuses.put(
              getFeatureStringRef(feature.getFeatureReference()),
              getMetadata(value, isOutsideMaxAge));
        } else {
          rowValues.put(getFeatureStringRef(featureReference), Value.newBuilder().build());

          rowStatuses.put(getFeatureStringRef(featureReference), getMetadata(null, false));
        }
      }
    }

    List<String> fieldNames =
        Stream.concat(
                entityNames.stream(), featureReferences.stream().map(this::getFeatureStringRef))
            .distinct()
            .sorted(Comparator.naturalOrder())
            .collect(Collectors.toList());

    return new RetrievedOnlineFeatures(entityRows, fieldNames, values, statuses);
  }

  String getFeatureStringRef(FeatureReference featureReference) {
    String ref = featureReference.getName();
    if (!featureReference.getFeatureTable().isEmpty()) {
      ref = featureReference.getFeatureTable() + ":" + ref;
    }
    return ref;
  }

  /**
   * Generate Field level Status metadata for the given valueMap.
   *
   * @param value value to generate metadata for.
   * @param isOutsideMaxAge whether the given valueMap contains values with age outside
   *     FeatureTable's max age.
   * @return a 1:1 map keyed by field name containing field status metadata instead of values in the
   *     given valueMap.
   */
  private static FieldStatus getMetadata(Value value, boolean isOutsideMaxAge) {

    if (value == null) {
      return FieldStatus.NOT_FOUND;
    } else if (isOutsideMaxAge) {
      return FieldStatus.OUTSIDE_MAX_AGE;
    } else if (value.getValCase().equals(Value.ValCase.VAL_NOT_SET)) {
      return FieldStatus.NULL_VALUE;
    }
    return FieldStatus.PRESENT;
  }

  private static Map<FeatureReference, Feature> getFeatureRefFeatureMap(List<Feature> features) {
    return features.stream()
        .collect(Collectors.toMap(Feature::getFeatureReference, Function.identity()));
  }

  /**
   * Determine if the feature data in the given feature row is outside maxAge. Data is outside
   * maxAge to be when the difference ingestion time set in feature row and the retrieval time set
   * in entity row exceeds FeatureTable max age.
   *
   * @param feature contains the ingestion timing and feature data.
   * @param entityRow contains the retrieval timing of when features are pulled.
   * @param maxAge feature's max age.
   */
  private static boolean checkOutsideMaxAge(
      Feature feature, GetOnlineFeaturesRequest.EntityRow entityRow, Duration maxAge) {

    if (maxAge.equals(Duration.getDefaultInstance())) { // max age is not set
      return false;
    }

    long givenTimestamp = entityRow.getTimestamp().getSeconds();
    if (givenTimestamp == 0) {
      givenTimestamp = System.currentTimeMillis() / 1000;
    }
    long timeDifference = givenTimestamp - feature.getEventTimestamp().getSeconds();
    return timeDifference > maxAge.getSeconds();
  }
}
