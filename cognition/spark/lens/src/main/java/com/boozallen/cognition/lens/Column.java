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

import org.apache.hadoop.io.Text;

import com.boozallen.cognition.accumulo.structure.Source;

import java.io.Serializable;

/**
 * Class representing a column within Cognition.
 * @author mparker
 *
 */
public class Column implements Serializable {
  private static final long serialVersionUID = 121L;

  private Source source;
  private String columnFamily;
  private String columnQualifier;

  public Column() {} //default constructor needed for deserialization

  /**
   * Initialize a column.
   * @param source -- the source of the data
   * @param cf -- the column family in accumulo
   * @param cq -- the column qualifier in accumulo
   */
  public Column(Source source, String cf, String cq) {
    this.source = source;
    this.columnFamily = cf;
    this.columnQualifier = cq;
  }

  /**
   * Initialize a column.
   * @param source -- the source of the data
   * @param cf -- the column family in accumulo
   * @param cq -- the column qualifier in accumulo
   */
  public Column(Source source, Text cf, Text cq) {
    this(source, cf.toString(), cq.toString());
  }

  /**
   * Return the source of the column
   * @return the source
   */
  public Source getSource() {
    return source;
  }
  
  /**
   * Return the column family
   * @return columnFamily
   */
  public Text getColumnFamily() {
    return new Text(columnFamily);
  }

  /**
   * Return the column qualifier
   * @return columnQualifier
   */
  public Text getColumnQualifier() {
    return new Text(columnQualifier);
  }

}
