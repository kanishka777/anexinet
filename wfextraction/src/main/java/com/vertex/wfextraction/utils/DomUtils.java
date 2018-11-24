package com.vertex.wfextraction.utils;

import org.w3c.dom.Document;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DomUtils {

  public static JsonObject loadJsonDocument(String resourceName) {

    JsonObject json = null;
    InputStream is = null;

    try {
      is = findInputStream(resourceName);

      JsonReader reader = Json.createReader(is);
      if (reader == null) {
        throw new RuntimeException("Configuration file not readable: " + resourceName);
      }

      json = reader.readObject();
      if (json == null) {
        throw new RuntimeException("Configuration file not properly formatted JSON: " + resourceName);
      }
    }
    finally {

      if (is != null) {
        try {
          is.close();
        }
        catch (IOException e) {
          throw new RuntimeException("Unknown error closing OAuth configuration file", e);
        }
      }
    }

    return json;
  }

  public static Document loadXmlDocument(String resourceName) {

    Document doc;

    try {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder db = dbf.newDocumentBuilder();
      doc = db.parse(findInputStream(resourceName));
    }
    catch (Exception e) {
      throw new RuntimeException("Unable to load document from named resource: " + resourceName, e);
    }

    return doc;
  }

  public static InputStream findInputStream(String resourceName) {

    InputStream is = ClassLoader.getSystemResourceAsStream(resourceName);
    if (is == null) {

      ClassLoader resourceLoader = Thread.currentThread().getContextClassLoader();
      is = resourceLoader.getResourceAsStream(resourceName);
      if (is == null) {

        try {
          is = new FileInputStream(resourceName);
        }
        catch (IOException e) {
          throw new RuntimeException(e.getLocalizedMessage(), e);
        }
      }
    }

    return is;
  }
}
