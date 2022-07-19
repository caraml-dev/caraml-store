package dev.caraml.store.util;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.caraml.store.protobuf.types.ValueProto.ValueType.Enum;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TypeConversion {
  private static Gson gson = new Gson();

  /**
   * Convert a java data object to protobuf Timestamp object
   *
   * @param ts timestamp
   * @return protobuf.Timestamp object of the given timestamp
   */
  public static com.google.protobuf.Timestamp convertTimestamp(Date ts) {
    return com.google.protobuf.Timestamp.newBuilder().setSeconds(ts.getTime() / 1000).build();
  }

  /**
   * Convert a string of comma-separated strings to list of strings
   *
   * @param tags comma separated tags
   * @return list of tags
   */
  public static List<String> convertTagStringToList(String tags) {
    if (tags == null || tags.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(tags.split(","));
  }

  /**
   * Unmarshals a given json string to map
   *
   * @param jsonString valid json formatted string
   * @return map of keys to values in json
   */
  public static Map<String, String> convertJsonStringToMap(String jsonString) {
    if (jsonString == null || jsonString.equals("") || jsonString.equals("{}")) {
      return Collections.emptyMap();
    }
    Type stringMapType = new TypeToken<Map<String, String>>() {}.getType();
    return gson.fromJson(jsonString, stringMapType);
  }

  /**
   * Unmarshals a given json string to Enum map
   *
   * @param jsonString valid json formatted string
   * @return map of keys to Enum values in json string
   */
  public static Map<String, Enum> convertJsonStringToEnumMap(String jsonString) {
    if (jsonString == null || jsonString.equals("") || jsonString.equals("{}")) {
      return Collections.emptyMap();
    }
    Type stringMapType = new TypeToken<Map<String, Enum>>() {}.getType();
    return gson.fromJson(jsonString, stringMapType);
  }

  /**
   * Marshals a given map into its corresponding json string
   *
   * @param map
   * @return json string corresponding to given map
   */
  public static String convertMapToJsonString(Map<String, String> map) {
    return gson.toJson(map);
  }
}
