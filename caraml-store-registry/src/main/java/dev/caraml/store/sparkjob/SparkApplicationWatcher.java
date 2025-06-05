package dev.caraml.store.sparkjob;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Watch;
import dev.caraml.store.sparkjob.crd.SparkApplication;
import dev.caraml.store.sparkjob.SparkOperatorApi;
import io.kubernetes.client.util.generic.options.ListOptions;
import com.google.gson.reflect.TypeToken;

class SparkApplicationWatcher {
    // private ApiClient k8sClient;
    // private SparkOperatorApi sparkOperatorApi;

    // public SparkApplicationWatcher(ClusterConfig cluster, String namespace) {
    //     this.k8sClient = cluster.getInCluster() ? ClientBuilder.cluster().build() : ClientBuilder.defaultClient();

    //     Watch<SparkApplication> watch = Watch.createWatch(
    //             k8sClient,
    //             this.sparkOperatorApi.list(namespace, ""),
    //             new TypeToken<Watch.Response<SparkApplication>>() {
    //             }.getType());
    // }

}
