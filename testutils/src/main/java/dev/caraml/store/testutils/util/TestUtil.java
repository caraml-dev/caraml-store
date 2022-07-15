package dev.caraml.store.testutils.util;

import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.core.OnlineStoreProto;
import java.util.HashSet;

public class TestUtil {

  /**
   * Compare if two Feature Table specs are equal. Disregards order of features/entities in spec.
   */
  public static boolean compareFeatureTableSpec(FeatureTableSpec spec, FeatureTableSpec otherSpec) {
    OnlineStoreProto.OnlineStore specOnlineStore = spec.getOnlineStore();
    OnlineStoreProto.OnlineStore otherSpecOnlineStore = otherSpec.getOnlineStore();

    //  "unset" online store should be reset so that they match
    if (spec.getOnlineStore().getName().equals("unset")) {
      specOnlineStore = OnlineStoreProto.OnlineStore.newBuilder().build();
    }

    if (otherSpec.getOnlineStore().getName().equals("unset")) {
      otherSpecOnlineStore = OnlineStoreProto.OnlineStore.newBuilder().build();
    }

    spec =
        spec.toBuilder()
            .clearFeatures()
            .addAllFeatures(new HashSet<>(spec.getFeaturesList()))
            .clearEntities()
            .addAllEntities(new HashSet<>(spec.getEntitiesList()))
            .clearOnlineStore()
            .setOnlineStore(specOnlineStore)
            .build();

    otherSpec =
        otherSpec.toBuilder()
            .clearFeatures()
            .addAllFeatures(new HashSet<>(otherSpec.getFeaturesList()))
            .clearEntities()
            .addAllEntities(new HashSet<>(otherSpec.getEntitiesList()))
            .clearOnlineStore()
            .setOnlineStore(otherSpecOnlineStore)
            .build();

    return spec.equals(otherSpec);
  }
}
