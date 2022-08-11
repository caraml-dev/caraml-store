package dev.caraml.store.sparkjob.crd;

import com.google.gson.annotations.SerializedName;

public enum DeployMode {
  @SerializedName("client")
  CLIENT,
  @SerializedName("cluster")
  CLUSTER,
  @SerializedName("in-cluster-client")
  IN_CLUSTER_CLIENT,
}
