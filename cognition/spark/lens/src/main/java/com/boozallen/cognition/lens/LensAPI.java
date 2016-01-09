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

import com.boozallen.cognition.accumulo.config.AccumuloConfiguration;
import com.boozallen.cognition.accumulo.config.CognitionConfiguration;
import com.boozallen.cognition.accumulo.structure.AccumuloUtils;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.serializer.KryoSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

/**
 * The central API for lens, allowing queries to accumulo
 * @author mparker
 *
 */
public class LensAPI {
  private SparkContext sc;
  private CognitionConfiguration cognition;

  /**
   * Create a lens api with the properties file on the classpath and creating a new spark context.
   */
  public LensAPI() {
    cognition = new CognitionConfiguration();
    sc = createSparkContext();
  }

  /**
   * Create a lens api with the properties file on the classpath and using an existing spark context.
   * @param -- the existing spark context
   */
  public LensAPI(SparkContext sc) {
    cognition = new CognitionConfiguration();
    this.sc = sc;
  }

  /**
   * Create a lens api with the given configuration and creating a new spark context.
   * @param cognition -- the cognition configuration to use
   */
  public LensAPI(CognitionConfiguration cognition) {
    this.cognition = cognition;
    sc = createSparkContext();
  }

  /**
   * Create a lens api with the given configuration and the given spark context
   * @param sc -- the existing spark context to use
   * @param cognition -- the cognition configuration to use
   */
  public LensAPI(SparkContext sc, CognitionConfiguration cognition) {
    this.cognition = cognition;
    this.sc = sc;
  }

  /**
   * Helper method for creating the spark context from the given cognition configuration
   * @return -- a new configured spark context
   */
  public SparkContext createSparkContext() {
    SparkConf conf = new SparkConf();

    Configuration config = cognition.getProperties();

    conf.set("spark.serializer", KryoSerializer.class.getName());
    conf.setAppName(config.getString("app.name"));
    conf.setMaster(config.getString("master"));

    Iterator<String> iterator = config.getKeys("spark");
    while (iterator.hasNext()) {
      String key = iterator.next();
      conf.set(key, config.getString(key));
    }

    SparkContext sc = new SparkContext(conf);
    for (String jar : config.getStringArray("jars")) {
      sc.addJar(jar);
    }

    return sc;
  }
  
  /**
   * Issue the query based on the given criteria.
   * @param criteria -- the criteria to filter by
   * @return a json string of the results
   */
  public String query(Criteria criteria) {
    Query query = new Query(sc, criteria, cognition);
    String json = query.json();
    return json;
  }

  /**
   * Issue the query based on the given criteria and limit.
   * @param criteria -- the criteria to filter by
   * @param limit -- the limit to the number of results returned
   * @return a json string of the results
   */
  public String query(Criteria criteria, int limit) {
    Query query = new Query(sc, criteria, cognition);
    String json = query.json(limit);
    return json;
  }

}
