package dev.caraml.store.sparkjob;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the job cluster-selection precedence: request argument &gt; job-type default &gt;
 * root default (represented by "", which the registry resolves to the root default cluster).
 */
public class JobServiceClusterResolutionTest {

  @Test
  public void requestArgumentWinsOverJobTypeDefault() {
    assertEquals("from-request", JobService.effectiveCluster("from-request", "from-template"));
  }

  @Test
  public void jobTypeDefaultUsedWhenNoRequestArgument() {
    assertEquals("from-template", JobService.effectiveCluster(null, "from-template"));
    assertEquals("from-template", JobService.effectiveCluster("", "from-template"));
  }

  @Test
  public void fallsBackToRootDefaultWhenNeitherSet() {
    assertEquals("", JobService.effectiveCluster(null, null));
    assertEquals("", JobService.effectiveCluster("", ""));
    assertEquals("", JobService.effectiveCluster("", null));
  }

  @Test
  public void requestArgumentWinsEvenWhenTemplateUnset() {
    assertEquals("from-request", JobService.effectiveCluster("from-request", null));
  }
}
