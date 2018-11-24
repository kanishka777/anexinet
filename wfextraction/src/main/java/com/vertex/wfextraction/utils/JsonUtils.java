package com.vertex.wfextraction.utils;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class JsonUtils {

  public static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";

  public static boolean hasNode(JsonObject jo, String nodeName) {
    assert jo != null;
    assert nodeName != null;

    return jo.containsKey(nodeName);
  }

  public static Object getValueByType(Class javaType, JsonValue value) {

    Object result = null;

    if ((value != null) && (value.getValueType() != JsonValue.ValueType.NULL)) {

      if (javaType == String.class) {
        result = JsonUtils.getString(value);
      }
      else if (javaType == Double.class) {
        result = JsonUtils.getDouble(value);
      }
      else if (javaType == Long.class) {
        result = JsonUtils.getLong(value);
      }
      else if (javaType == Date.class) {
        result = JsonUtils.getDate(value);
      }
      else if (javaType == Boolean.class) {
        result = JsonUtils.getBoolean(value);
      }
      else {
        assert false : "Unrecognized data type for value conversion: " + javaType.getName();
      }
    }

    return result;
  }

  public static String getString(JsonObject jo, String name) {

    assert jo != null;
    assert name != null;

    String str = null;

    if (jo.containsKey(name)) {

      JsonValue value = jo.get(name);
      if (value != null) {
        str = getString(value);
      }
    }

    return str;
  }

  public static String getString(JsonValue value) {

    String str;

    if (value.getValueType() == JsonValue.ValueType.STRING) {
      str = ((JsonString)value).getString();
    }
    else if (value.getValueType() == JsonValue.ValueType.NUMBER) {
      str = value.toString();
    }
    else if (value.getValueType() == JsonValue.ValueType.FALSE) {
      str = Boolean.FALSE.toString();
    }
    else if (value.getValueType() == JsonValue.ValueType.TRUE) {
      str = Boolean.TRUE.toString();
    }
    else if (value.getValueType() == JsonValue.ValueType.NULL) {
      str = null;
    }
    else {
      throw new RuntimeException("Unrecognized value type for string: " + value.getValueType());
    }

    return str;
  }

  public static boolean getBoolean(JsonObject jo, String name) {

    assert jo != null;
    assert name != null;

    boolean flag = false;

    if (jo.containsKey(name)) {

      JsonValue value = jo.get(name);
      if (value != null) {
        flag = getBoolean(value);
      }
    }

    return flag;
  }

  public static boolean getBoolean(JsonValue value) {

    boolean flag;

    if (value.getValueType() == JsonValue.ValueType.STRING) {
      String str = ((JsonString)value).getString();
      flag = Boolean.TRUE.toString().equalsIgnoreCase(str);
    }
    else if (value.getValueType() == JsonValue.ValueType.FALSE) {
      flag = false;
    }
    else if (value.getValueType() == JsonValue.ValueType.TRUE) {
      flag = true;
    }
    else {
      throw new RuntimeException("Unrecognized value type for boolean: " + value.getValueType());
    }

    return flag;
  }

  public static long getLong(JsonObject jo, String name) {

    assert jo != null;
    assert name != null;

    long number = 0;

    if (jo.containsKey(name)) {

      JsonValue value = jo.get(name);
      if (value != null) {
        number = getLong(value);
      }
    }

    return number;
  }

  public static long[] getLongArray(JsonObject base, String name) {

    assert base != null;
    assert name != null;

    long[] numbers = null;

    if (base.containsKey(name)) {

      JsonArray array = base.getJsonArray(name);
      numbers = new long[array.size()];

      for (int index = 0; index < array.size(); index++) {
        JsonValue value = array.get(index);
        if (value != null) {
          numbers[index] = getLong(value);
        }
      }
    }

    return numbers;
  }

  public static long getLong(JsonValue value) {

    long number;

    if (value.getValueType() == JsonValue.ValueType.STRING) {
      String str = ((JsonString)value).getString();
      number = Long.parseLong(str);
    }
    else if (value.getValueType() == JsonValue.ValueType.NUMBER) {
      JsonNumber jn = (JsonNumber)value;
      number = jn.longValue();
    }
    else if (value.getValueType() == JsonValue.ValueType.FALSE) {
      number = 0;
    }
    else if (value.getValueType() == JsonValue.ValueType.TRUE) {
      number = 1;
    }
    else {
      throw new RuntimeException("Unrecognized value type for long: " + value.getValueType());
    }

    return number;
  }

  public static double getDouble(JsonObject jo, String name) {

    assert jo != null;
    assert name != null;

    double number = 0.0;

    if (jo.containsKey(name)) {

      JsonValue value = jo.get(name);
      if (value != null) {
        number = getDouble(value);
      }
    }

    return number;
  }

  public static double getDouble(JsonValue value) {

    double number;

    if (value.getValueType() == JsonValue.ValueType.STRING) {
      String str = ((JsonString)value).getString();
      number = Double.parseDouble(str);
    }
    else if (value.getValueType() == JsonValue.ValueType.NUMBER) {
      JsonNumber jn = (JsonNumber)value;
      number = jn.doubleValue();
    }
    else {
      throw new RuntimeException("Unrecognized value type for double: " + value.getValueType());
    }

    return number;
  }

  public static Date getDate(JsonObject jo, String name) {

    assert jo != null;
    assert name != null;

    Date date = null;

    if (jo.containsKey(name)) {

      JsonValue value = jo.get(name);
      if (value != null) {
        date = getDate(value);
      }
    }

    return date;
  }

  public static Date getDate(JsonValue value) {

    Date date = null;
    if (value.getValueType() == JsonValue.ValueType.STRING) {
      String str = ((JsonString)value).getString();
      try {
        date = new SimpleDateFormat(DATE_FORMAT_PATTERN).parse(str);
      }
      catch (ParseException e) {
        throw new RuntimeException("Unable to parse date string (" + DATE_FORMAT_PATTERN + "): " + str);
      }
    }
    else if (value.getValueType() != JsonValue.ValueType.NULL) {
      throw new RuntimeException("Unrecognized value type for date: " + value.getValueType());
    }

    return date;
  }

  public static String[] getStringArray(JsonObject jo, String name) {

    assert jo != null;
    assert name != null;

    String[] values = null;

    JsonValue arrayObj = jo.get(name);
    if ((arrayObj != null) && (arrayObj.getValueType() == JsonValue.ValueType.ARRAY)) {

      JsonArray array = (JsonArray)arrayObj;
      values = new String[array.size()];
      for (int index = 0; index < array.size(); index++) {

        JsonValue value = array.get(index);
        if (value.getValueType() == JsonValue.ValueType.STRING) {
          values[index] = ((JsonString)value).getString();
        }
      }
    }

    return values;
  }

  public static String toJson(Map<String,Object> params) {

    StringBuilder sb = new StringBuilder();
    toJson(params, sb, 0);
    return sb.toString();
  }

  private static void addIndent(StringBuilder sb, int count) {
    while (count-- > 0) {
      sb.append('\t');
    }
  }

  private static void toJson(Map<String,Object> params, StringBuilder sb, int indent) {

    addIndent(sb, indent);
    sb.append("{\n");

    boolean first = true;
    for (Map.Entry<String,Object> entry : params.entrySet()) {

      if (!first) {
        sb.append(",\n");
      }
      else {
        first = false;
      }

      String name = entry.getKey();
      Object value = entry.getValue();
      Class type = (value != null) ? value.getClass() : null;

      if ((value != null) && (value instanceof List)) {
        assert false;
      }
      else if ((value != null) && (value instanceof Map)) {
        assert false;
      }
      else {
        addIndent(sb, indent + 1);
        sb.append('\"');
        sb.append(name);
        sb.append("\": ");

        if (value == null) {
          sb.append("null");
        }
        else if ((type == Integer.class) || (type == Long.class) || (type == Double.class) || (type == Boolean.class)) {
          sb.append(value.toString());
        }
        else if (type == String.class) {
          sb.append('\"');
          sb.append((String)value);
          sb.append('\"');
        }
        else if (type == Date.class) {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
          sb.append('\"');
          sb.append(sdf.format((Date)value));
          sb.append('\"');
        }
        else if (type == Timestamp.class) {
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
          sb.append('\"');
          sb.append(sdf.format((Timestamp)value));
          sb.append('\"');
        }
        else {
          sb.append('\"');
          sb.append(value.toString());
          sb.append('\"');
        }
      }
    }

    sb.append("\n");
    addIndent(sb, indent);
    sb.append("}\n");
  }
}
