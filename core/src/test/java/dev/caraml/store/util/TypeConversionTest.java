package dev.caraml.store.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Timestamp;
import java.util.*;
import org.junit.jupiter.api.Test;

public class TypeConversionTest {
  @Test
  public void convertTimeStampShouldCorrectlyConvertDateToProtobufTimestamp() {
    Date date = new Date(1000);
    Timestamp expected = Timestamp.newBuilder().setSeconds(1).build();
    assertEquals(TypeConversion.convertTimestamp(date), expected);
  }

  @Test
  public void convertTagStringToListShouldConvertTagStringToList() {
    String input = "value1,value2";
    List<String> expected = Arrays.asList("value1", "value2");
    assertEquals(TypeConversion.convertTagStringToList(input), expected);
  }

  @Test
  public void convertTagStringToListShouldReturnEmptyListForEmptyString() {
    String input = "";
    List<String> expected = Collections.emptyList();
    assertEquals(TypeConversion.convertTagStringToList(input), expected);
  }

  @Test
  public void convertJsonStringToMapShouldConvertJsonStringToMap() {
    String input = "{\"key\": \"value\"}";
    Map<String, String> expected = new HashMap<>();
    expected.put("key", "value");
    assertEquals(TypeConversion.convertJsonStringToMap(input), expected);
  }

  @Test
  public void convertJsonStringToMapShouldReturnEmptyMapForEmptyJson() {
    String input = "{}";
    Map<String, String> expected = Collections.emptyMap();
    assertEquals(TypeConversion.convertJsonStringToMap(input), expected);
  }

  @Test
  public void convertMapToJsonStringShouldReturnJsonStringForGivenMap() {
    Map<String, String> input = new HashMap<>();
    input.put("key", "value");
    assertEquals(TypeConversion.convertMapToJsonString(input), "{\"key\":\"value\"}");
  }

  @Test
  public void convertMapToJsonStringShouldReturnEmptyJsonForAnEmptyMap() {
    Map<String, String> input = new HashMap<>();
    assertEquals(TypeConversion.convertMapToJsonString(input), "{}");
  }
}
