package dev.caraml.store.sparkjob;

import com.google.gson.annotations.SerializedName;

public enum SecretType {
  @SerializedName("GCPServiceAccount")
  GCP_SERVICE_ACCOUNT,
  @SerializedName("Generic")
  GENERIC,
  @SerializedName("HadoopDelegationToken")
  HADOOP_DELEGATION_TOKEN
}
