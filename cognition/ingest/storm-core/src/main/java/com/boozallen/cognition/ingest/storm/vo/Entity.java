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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * Class for PIP extracted Entities
 */
public class Entity extends AccumuloItem {

  private String id;
  private Map<String, String> metaProps;
  private Map<String, String> properties;

  /**
   * Construct a blank entity.
   */
  public Entity() {
  }

  /**
   * Construct an entity with the given id, meta properties and entity properties.
   *
   * @param id
   * @param metaProps
   * @param properties
   */
  public Entity(String id, Map<String, String> metaProps, Map<String, String> properties) {
    this.id = id;
    this.metaProps = metaProps;
    this.properties = properties;
  }

  /**
   * Get the id for this entity.
   *
   * @return
   */
  public String getId() {
    return id;
  }

  /**
   * Set the id for this entity
   *
   * @param id
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Get the meta properties map for this entity
   *
   * @return
   */
  public Map<String, String> getMetaProps() {
    return metaProps;
  }

  /**
   * Set the meta properties map for this entity
   *
   * @param metaProps
   */
  public void setMetaProps(Map<String, String> metaProps) {
    this.metaProps = metaProps;
  }

  /**
   * Puts a single meta property.
   *
   * @param key
   * @param value
   */
  public void putMetaProp(String key, String value) {
    if (this.metaProps == null)
      metaProps = new LinkedHashMap<String, String>();
    metaProps.put(key, value);
  }

  /**
   * Get the properties map for this entity
   *
   * @return
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Set the properties map for this entity
   *
   * @param properties
   */
  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  /**
   * Create a mutation which represents this entity in accumulo.
   *
   * @return
   * @throws InvalidItemException
   */
  @Override
  protected Mutation createMutation() throws InvalidItemException {
    if (id == null) {
      throw new InvalidItemException("Id cannot be null.");
    }
    Mutation out = new Mutation(id);
    writeMap(out, "meta", metaProps);
    writeMap(out, "item", properties);
    return out;
  }

  /**
   * Create a mutation which represents this entity in accumulo.
   *
   * @param vis Accumulo entry visibility
   * @return
   * @throws InvalidItemException
   */
  @Override
  protected Mutation createMutation(ColumnVisibility vis) throws InvalidItemException {
    if (id == null) {
      throw new InvalidItemException("Id cannot be null.");
    }
    Mutation out = new Mutation(id);
    writeMap(out, "meta", metaProps, vis);
    writeMap(out, "item", properties, vis);
    return out;
  }

  /**
   * Use the provided <code>row</code> to fill in the fields of this entity.
   *
   * @param row
   */
  @Override
  public void populate(SortedMap<Key, Value> row) {
    id = row.firstKey().getRow().toString();
    metaProps = readMap(row, "meta", true);
    properties = readMap(row, "item", true);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }
}
