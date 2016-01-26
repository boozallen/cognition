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

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

/**
 * Object holding all query information to execute cognition queries.
 * @author mparker
 *
 */
public class Criteria implements Serializable {
  private static final long serialVersionUID = 9028294022408214389L;
  private static final boolean CASE_SENSITIVE = false;

  private Instant dateStart;
  private Instant dateEnd;
  private Map<Field, String> stringMatches;
  private Collection<String> keywords;
  private boolean useSpaceTokens = true;
  private boolean caseSensitive = CASE_SENSITIVE;

  private SchemaAdapter schema;
  private String accumuloTable;

  //TODO add support for regex's
  //TODO add support for multiple matches (or'd matches)

  public Criteria() {
    this.accumuloTable = "";

    this.dateStart = Instant.MIN;
    this.dateEnd = Instant.MAX;

    //note: enum map does not serialize with kryo for spark so use HashMap
    this.stringMatches = new HashMap<>();
    this.keywords = new HashSet<>();

  }

  public Criteria useSpaceTokens(boolean useSpaceTokens) {
    this.useSpaceTokens = useSpaceTokens;
    return this;
  }

  public boolean getUseSpaceTokens() {
    return useSpaceTokens;
  }

  public Criteria addMatch(Field field, String criteria) {
    String transformed = criteria;
    if (!caseSensitive) {
      transformed = transformed.toLowerCase();
    }
    this.stringMatches.put(field, transformed);
    return this;
  }

  public Criteria addKeyword(String keyword) {
    this.keywords.add(keyword);
    return this;
  }

  public Criteria addKeywords(List<String> keywords) {
    this.keywords.addAll(keywords);
    return this;
  }

  public Criteria setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
    return this;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public Collection<String> getKeywords() {
    return getTransformedKeywords();
  }

  //keywords list assumed to be small enough that this does not cause a
  //performance hindrance
  private Collection<String> getTransformedKeywords() {
    Collection<String> transformed = new HashSet<>();
    for (String keyword : keywords) {
      if (useSpaceTokens) {
        keyword = " " + keyword + " ";
      }
      if (!caseSensitive) {
        keyword = keyword.toLowerCase();
      }
      transformed.add(keyword);
    }
    return transformed;
  }

  public Criteria setDates(Instant start, Instant end) {
    this.dateStart = start;
    this.dateEnd = end;
    return this;
  }

  public Instant getDateStart() {
    return dateStart;
  }

  public Instant getDateEnd() {
    return dateEnd;
  }

  public Map<Field, String> getStringMatches() {
    Map<Field, String> matches = new HashMap<>();

    // Not very efficient to do this here (in getter) but because the
    // caseSensitive flag can be switched on and off, this code needs to be here
    Set<Field> keys = stringMatches.keySet();
    for (Field key : keys) {
      if (!caseSensitive) {
        matches.put(key, stringMatches.get(key).toLowerCase());
      } else {
        matches.put(key, stringMatches.get(key));
      }
    }

    return matches;
  }

  public SchemaAdapter getSchema() {
    return schema;
  }

  public Criteria setSchema(SchemaAdapter schema) {
    this.schema = schema;
    return this;
  }

  public String getAccumuloTable() {
    //default read from schema definition
    if(accumuloTable == null || accumuloTable.isEmpty()){
      if(getSchema() != null){
        return getSchema().getTableName();
      }
    }
    return accumuloTable;
  }

  public Criteria setAccumuloTable(String accumuloTable) {
    this.accumuloTable = accumuloTable;
    return this;
  }

  @Override
  public String toString() {
    return "Criteria [accumuloTable=" + accumuloTable + ", dateStart=" + dateStart + ", dateEnd=" + dateEnd
        + ", stringMatches=" + stringMatches + ", keywords=" + keywords
        + ", useSpaceTokens=" + useSpaceTokens + ", caseSensitive=" + caseSensitive + ", schema=" + schema
        + "]";
  }

}
