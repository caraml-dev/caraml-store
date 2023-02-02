package dev.caraml.store.sparkjob;

import static org.junit.jupiter.api.Assertions.*;

import dev.caraml.store.sparkjob.crd.SparkApplicationSpec;
import dev.caraml.store.sparkjob.crd.SparkExecutorSpec;
import io.kubernetes.client.openapi.models.*;
import java.util.List;
import org.junit.jupiter.api.Test;

class IngestionJobTemplateTest {

  @Test
  void shouldReplaceKeyword() {
    SparkApplicationSpec applicationSpec = new SparkApplicationSpec();
    SparkExecutorSpec executorSpec = new SparkExecutorSpec();
    V1Affinity affinity = new V1Affinity();
    V1PodAntiAffinity antiAffinity = new V1PodAntiAffinity();
    V1PodAffinityTerm podAffinityTerm = new V1PodAffinityTerm();
    V1LabelSelector labelSelector = new V1LabelSelector();
    V1LabelSelectorRequirement featureTableMatchExpression = new V1LabelSelectorRequirement();
    featureTableMatchExpression.setKey("caraml.dev/table");
    featureTableMatchExpression.setOperator("In");
    featureTableMatchExpression.setValues(List.of("${featureTable}"));
    labelSelector.addMatchExpressionsItem(featureTableMatchExpression);
    V1LabelSelectorRequirement projectMatchExpression = new V1LabelSelectorRequirement();
    projectMatchExpression.setKey("caraml.dev/project");
    projectMatchExpression.setOperator("In");
    projectMatchExpression.setValues(List.of("${project}"));
    labelSelector.addMatchExpressionsItem(projectMatchExpression);
    podAffinityTerm.setLabelSelector(labelSelector);
    podAffinityTerm.setTopologyKey("kubernetes.io/hostname");
    antiAffinity.addRequiredDuringSchedulingIgnoredDuringExecutionItem(podAffinityTerm);
    affinity.setPodAntiAffinity(antiAffinity);
    executorSpec.setAffinity(affinity);
    applicationSpec.setExecutor(executorSpec);
    IngestionJobTemplate template = new IngestionJobTemplate("store", applicationSpec);
    SparkApplicationSpec renderedSpec = template.render("some_project", "some_table");
    V1PodAffinityTerm renderedPodAffinityTerm =
        renderedSpec
            .getExecutor()
            .getAffinity()
            .getPodAntiAffinity()
            .getRequiredDuringSchedulingIgnoredDuringExecution()
            .get(0);
    List<V1LabelSelectorRequirement> renderedMatchExpressions =
        renderedPodAffinityTerm.getLabelSelector().getMatchExpressions();
    assertEquals("some_table", renderedMatchExpressions.get(0).getValues().get(0));
    assertEquals("some_project", renderedMatchExpressions.get(1).getValues().get(0));
    assertEquals("kubernetes.io/hostname", renderedPodAffinityTerm.getTopologyKey());
  }
}
