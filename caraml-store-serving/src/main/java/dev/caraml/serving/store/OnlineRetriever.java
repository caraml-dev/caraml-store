package dev.caraml.serving.store;

import dev.caraml.store.protobuf.serving.ServingServiceProto.FeatureReference;
import dev.caraml.store.protobuf.serving.ServingServiceProto.GetOnlineFeaturesRequest.EntityRow;
import java.util.List;

public interface OnlineRetriever {
  /**
   * Get online features for the given entity rows using data retrieved from the Feature references
   * specified in FeatureTable request.
   *
   * <p>Each {@link Feature} optional in the returned list then corresponds to an {@link EntityRow}
   * provided by the user. If feature for a given entity row is not found, will return an empty
   * optional instead. The no. of {@link Feature} returned should match the no. of given {@link
   * EntityRow}s
   *
   * @param project name of project to request features from.
   * @param entityRows list of entity rows to request features for.
   * @param featureReferences specifies the FeatureTable to retrieve data from
   * @return list of {@link Feature}s corresponding to data retrieved for each entity row from
   *     FeatureTable specified in FeatureTable request.
   */
  List<List<Feature>> getOnlineFeatures(
      String project,
      List<EntityRow> entityRows,
      List<FeatureReference> featureReferences,
      List<String> entityNames);
}
