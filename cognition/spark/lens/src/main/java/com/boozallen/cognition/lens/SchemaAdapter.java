/*
 * Licensed to Booz Allen Hamilton under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Booz Allen Hamilton licenses this file to you
 * under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.boozallen.cognition.lens;

import com.boozallen.cognition.accumulo.structure.Source;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import scala.Tuple2;

import java.io.*;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a means of converting the underlying accumulo schema to a 
 * more modular language.
 * @author mparker
 *
 */
public class SchemaAdapter implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(SchemaAdapter.class);
  private static final long serialVersionUID = 1L;
  private Map<Field, List<Column>> fields = new EnumMap<>(Field.class);
  private Map<Property, String> properties = new EnumMap<>(Property.class);
  private static Gson gson = new Gson();

  public enum Property {
    TABLE_NAME, AUTHORIZATIONS;
  }

  public SchemaAdapter() {
  }

  public SchemaAdapter(SchemaAdapter s) {
    this.fields = s.fields;
    this.properties = s.properties;
  }

  public String getTableName() {
    return properties.get(Property.TABLE_NAME);
  }

  public String getAuthorizations() {
    return properties.get(Property.AUTHORIZATIONS);
  }

  public void setTableName(String tableName) {
    properties.put(Property.TABLE_NAME, tableName);
  }

  public void setAuthorizations(String authorizations) {
    properties.put(Property.AUTHORIZATIONS, authorizations);
  }

  /**
   * Reads a file located on the classpath and converts it to a string.
   * @param path
   * @return a string of the file's contents
   */
  public String readFileOnClasspath(String path) {
    String fileContents = null;
    try {
      InputStream file = getClass().getClassLoader().getResourceAsStream(path);
      BufferedReader reader = new BufferedReader(new InputStreamReader(file));
      StringBuilder out = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        out.append(line);
      }
      fileContents = out.toString();   //Prints the string content read from input stream
      reader.close();
    } catch (IOException e) {
      logger.error(e.getMessage(),e);
    }
    return fileContents;
  }

  /**
   * Loads a json file from a path on the classpath into a schema adapter.
   * @param path -- the path on the classpath
   */
  public void loadJson(String path) {
    loadJsonString(readFileOnClasspath(path));
  }

  /**
   * Loads a json string into a schema adapter.
   * @param json -- the json string to load
   */
  public void loadJsonString(String json) {
    SchemaAdapter s = null;
    try {
      s = gson.fromJson(json, SchemaAdapter.class);
    } catch (JsonSyntaxException | JsonIOException e) {
      logger.error(e.getMessage(),e);
    }
    this.fields = s.fields;
    this.properties = s.properties;
  }

  /**
   * Returns a json representation of this object.
   * @return -- a json string of this schema adapter object
   */
  public String getJson() {
    return gson.toJson(this);
  }

  /**
   * Add a field and column to adapt
   * @param field -- the higher level field to map to
   * @param column -- the accumlo representation of the field
   */
  public void addField(Field field, Column column) {
    if (!fields.containsKey(field)) {
      fields.put(field, new ArrayList<Column>());
    }
    fields.get(field).add(column);
  }

  /**
   * Return the field based on known accumulo column family and qualifiers.
   * @param cf -- the accumulo column family
   * @param cq -- the accumulo column qualifier
   * @return the field associated with the given parameters or null if there is not one
   */
  public Field getField(String cf, String cq) {
    for (Map.Entry<Field, List<Column>> field : fields.entrySet()) {
      for (Column column : field.getValue()) {
        if (column.getColumnFamily().toString().equals(cf) &&
            column.getColumnQualifier().toString().equals(cq)) {
          return field.getKey();
        }
      }
    }
    return null;
  }

  /**
   * Return the field based on known accumulo column family and qualifiers.
   * @param source -- the source of the data
   * @param cf -- the accumulo column family
   * @param cq -- the accumulo column qualifier
   * @return the field associated with the given parameters or null if there is not one
   */
  public Field getField(Source source, String cf, String cq) {
    for (Map.Entry<Field, List<Column>> field : fields.entrySet()) {
      for (Column column : field.getValue()) {
        if (column.getColumnFamily().toString().equals(cf) &&
            column.getColumnQualifier().toString().equals(cq) &&
            column.getSource().equals(source)) {
          return field.getKey();
        }
      }
    }
    return null;
  }

  /**
   * Return all column family column qualifier pairs based on the source of the data
   * @param source -- the source of the data
   * @return a list of tuples of (cf,cq)
   */
  public List<Tuple2<String, String>> getTuples(Source source) {
    List<Tuple2<String, String>> tuples = new ArrayList<>();
    for (Map.Entry<Field, List<Column>> field : fields.entrySet()) {
      for (Column column : field.getValue()) {
        if (column.getSource().equals(source)) {
          tuples.add(new Tuple2<String, String>(column.getColumnFamily().toString(), column.getColumnQualifier().toString()));
        }
      }
    }
    return tuples;
  }

  /**
   * Return all column family column qualifier pairs based on the source of the data and the field
   * @param field -- the associated field
   * @param source -- the source of the data
   * @return a list of tuples of (cf,cq)
   */
  public List<Tuple2<String, String>> getTuples(Field field, Source source) {
    List<Tuple2<String, String>> tuples = new ArrayList<>();
    for (Column column : fields.get(field)) {
      if (column.getSource().equals(source)) {
        tuples.add(new Tuple2<String, String>(column.getColumnFamily().toString(), column.getColumnQualifier().toString()));
      }
    }
    return tuples;
  }

  /**
   * Return all columns based on the source of the data and the field
   * @param field -- the associated field
   * @param source -- the source of the data
   * @return a list of columns
   */
  public List<Column> getColumns(Field field, Source source) {
    List<Column> columns = new ArrayList<>();
    for (Column column : fields.get(field)) {
      if (column.getSource().equals(source)) {
        columns.add(column);
      }
    }
    return columns;
  }

}
