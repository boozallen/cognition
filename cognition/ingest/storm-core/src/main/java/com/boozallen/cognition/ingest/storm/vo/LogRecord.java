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

package com.boozallen.cognition.ingest.storm.vo;

import java.util.*;

/**
 * This class is essentially a wrapper for two Map<String,String> objects to be passed through a Storm topology.
 */
public class LogRecord implements java.io.Serializable {
  private Map<String, String> _fields;
  private Map<String, String> _metadata;
  private final String _uuid;
  private Date _logDate;

  public LogRecord() {
    this(UUID.randomUUID().toString());
  }

  public LogRecord(String id) {
    this._uuid = id;
    _fields = new LinkedHashMap<String, String>();
    _metadata = new LinkedHashMap<String, String>();
    _logDate = new Date();
  }

  public LogRecord(String id, LogRecord logRecord) {
    this._uuid = id;
    _fields = logRecord._fields;
    _metadata = logRecord._metadata;
    _logDate = logRecord._logDate;
  }

  public LogRecord(Map<String, String> records) {
    this();
    _fields.putAll(records);
  }

  public Date getDate() {
    return _logDate;
  }

  public void setDate(Date date) {
    _logDate = date;
  }

  public String getUUID() {
    return _uuid;
  }

  public Map<String, String> getFields() {
    return _fields;
  }

  public Map<String, String> getMetadata() {
    return _metadata;
  }

  public List<String> getFieldNames() {
    List<String> fields = new ArrayList<String>();
    fields.addAll(_fields.keySet());
    return Collections.unmodifiableList(fields);
  }

  public List<String> getMetadataFieldNames() {
    List<String> metaDataFields = new ArrayList<String>();
    metaDataFields.addAll(_metadata.keySet());
    return Collections.unmodifiableList(metaDataFields);
  }

  public String getValue(String name) {
    return _fields.get(name);
  }

  public void setValue(String name, String value) {
    _fields.put(name, value);
  }

  public String getMetadataValue(String name) {
    return _metadata.get(name);
  }

  public void removeField(String name) {
    _fields.remove(name);
  }

  public Boolean hasField(String name) {
    return _fields.containsKey(name);
  }

  public void addMetadataValue(String name, String value) {
    _metadata.put(name, value);
  }

  public void removeMetadataField(String name) {
    _metadata.remove(name);
  }
}
