package dev.caraml.store.feature;

public interface ProjectValidator {

  void validateProject(String project) throws IllegalArgumentException;
}
