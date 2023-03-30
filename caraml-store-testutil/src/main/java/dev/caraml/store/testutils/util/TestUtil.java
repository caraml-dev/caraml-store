package dev.caraml.store.testutils.util;

import dev.caraml.store.protobuf.core.FeatureProto;
import dev.caraml.store.protobuf.core.FeatureTableProto.FeatureTableSpec;
import dev.caraml.store.protobuf.core.OnlineStoreProto;
import java.util.Comparator;

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
            .addAllFeatures(
                spec.getFeaturesList().stream()
                    .sorted(Comparator.comparing(FeatureProto.FeatureSpec::getName))
                    .toList())
            .clearEntities()
            .addAllEntities(
                spec.getEntitiesList().stream().sorted(Comparator.naturalOrder()).toList())
            .clearOnlineStore()
            .setOnlineStore(specOnlineStore)
            .build();

    otherSpec =
        otherSpec.toBuilder()
            .clearFeatures()
            .addAllFeatures(
                spec.getFeaturesList().stream()
                    .sorted(Comparator.comparing(FeatureProto.FeatureSpec::getName))
                    .toList())
            .clearEntities()
            .addAllEntities(
                spec.getEntitiesList().stream().sorted(Comparator.naturalOrder()).toList())
            .clearOnlineStore()
            .setOnlineStore(otherSpecOnlineStore)
            .build();

    boolean equals = spec.equals(otherSpec);
    if (!equals) {
      System.out.println(spec);
      System.out.println(otherSpec);
    }
    return equals;
  }
}
