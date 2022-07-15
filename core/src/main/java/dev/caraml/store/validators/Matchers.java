package dev.caraml.store.validators;

import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Pattern;

public class Matchers {

  private static final Pattern BIGQUERY_TABLE_REF_REGEX =
      Pattern.compile("[a-zA-Z0-9-]+[:]+[a-zA-Z0-9_]+[.]+[a-zA-Z0-9_]*");
  private static final Pattern CLASS_PATH_REGEX =
      Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)*$");
  private static final Pattern UPPER_SNAKE_CASE_REGEX = Pattern.compile("^[A-Z0-9]+(_[A-Z0-9]+)*$");
  private static final Pattern LOWER_SNAKE_CASE_REGEX = Pattern.compile("^[a-z0-9]+(_[a-z0-9]+)*$");
  private static final Pattern VALID_CHARACTERS_REGEX = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");
  private static final Pattern VALID_CHARACTERS_REGEX_WITH_DASH =
      Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_-]*$");

  private static final String ERROR_MESSAGE_TEMPLATE = "invalid value for %s resource, %s: %s";

  public static void checkUpperSnakeCase(String input, String resource)
      throws IllegalArgumentException {
    if (!UPPER_SNAKE_CASE_REGEX.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE,
              resource,
              input,
              "argument must be in upper snake case, and cannot include any special characters."));
    }
  }

  public static void checkLowerSnakeCase(String input, String resource)
      throws IllegalArgumentException {
    if (!LOWER_SNAKE_CASE_REGEX.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE,
              resource,
              input,
              "argument must be in lower snake case, and cannot include any special characters."));
    }
  }

  public static void checkValidCharacters(String input, String resource)
      throws IllegalArgumentException {
    if (!VALID_CHARACTERS_REGEX.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE,
              resource,
              input,
              "argument must only contain alphanumeric characters and underscores."));
    }
  }

  public static void checkValidCharactersAllowDash(String input, String resource)
      throws IllegalArgumentException {
    if (!VALID_CHARACTERS_REGEX_WITH_DASH.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE,
              resource,
              input,
              "argument must only contain alphanumeric characters, dashes, or underscores."));
    }
  }

  public static void checkValidBigQueryTableRef(String input, String resource)
      throws IllegalArgumentException {
    if (!BIGQUERY_TABLE_REF_REGEX.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE,
              resource,
              input,
              "argument must be in the form of <project:dataset.table> ."));
    }
  }

  public static void checkValidClassPath(String input, String resource) {
    if (!CLASS_PATH_REGEX.matcher(input).matches()) {
      throw new IllegalArgumentException(
          String.format(
              ERROR_MESSAGE_TEMPLATE, resource, input, "argument must be a valid Java Classpath"));
    }
  }

  public static boolean hasDuplicates(Collection<String> strings) {
    return (new HashSet<>(strings)).size() < strings.size();
  }
}
