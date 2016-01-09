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

package com.boozallen.cognition.accumulo.config;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.IOException;
import java.io.Serializable;

/**
 * The configuration container for all cognition parts. Currently, it holds only
 * accumulo configurations but this may change in the future.
 * 
 * @author mparker
 *
 */
public class CognitionConfiguration implements Serializable {
  private static final long serialVersionUID = -8422385735368244735L;
  public static final String LENS_API_PROPERTIES = "lens-api.properties";

  private AccumuloConfiguration accumuloConfig;
  private Configuration config;

  /**
   * Load the CognitionConfiguration from the default properties file.
   */
  public CognitionConfiguration() {
    this(LENS_API_PROPERTIES);
  }


  /**
   * Load the CognitionConfiguration from the given properties file.
   * 
   * @param filePath -- the path to the properties file
   */
  public CognitionConfiguration(String filePath) {
    try {
      this.config = new PropertiesConfiguration(filePath);
      this.accumuloConfig = new AccumuloConfiguration(config.getString("zookeeper.instance.name"),
          config.getString("zookeeper.instance.hosts"), config.getString("accumulo.user"),
          config.getString("accumulo.password"));
    } catch (ConfigurationException | AccumuloSecurityException | IOException e) {
      throw new RuntimeException("Unable to load configuration or configuration is invalid: " + filePath);
    }
  }

  /**
   * Load the CognitionConfiguration from the given AccumuloConfiguration object
   * and any other configurations from the default properties file.
   */
  public CognitionConfiguration(AccumuloConfiguration accumuloConfig) {
    this(accumuloConfig, LENS_API_PROPERTIES);
  }

  /**
   * Load the CognitionConfiguration from the given AccumuloConfiguration object
   * and any other configurations from the given properties file.
   * @param filePath -- the path to the properties file
   */
  public CognitionConfiguration(AccumuloConfiguration accumuloConfig, String filePath) {
    try {
      this.config = new PropertiesConfiguration(filePath);
    } catch (ConfigurationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    this.accumuloConfig = accumuloConfig;
  }

  /**
   * Returns the properties object with the current cognition properties.
   */
  public Configuration getProperties() {
    return config;
  }

  /**
   * Returns the accumulo configuration object with all current accumulo properties.
   */
  public AccumuloConfiguration getAccumuloConfiguration() {
    return this.accumuloConfig;
  }

}
