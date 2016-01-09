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

package com.boozallen.cognition.ingest.accumulo.utils;

import org.apache.commons.configuration.Configuration;

/**
 * Utility for extracting accumulo configuration from {@link Configuration}
 */
public class AccumuloConnectionUtils {
  private static final String INSTANCE = "accumuloConfig.instanceName";
  private static final String ZOO_SERVERS = "accumuloConfig.zooServers";
  private static final String USER = "accumuloConfig.user";
  private static final String KEY = "accumuloConfig.key";
  private static final String MAX_MEM = "maxMem";
  private static final String MAX_LATENCY = "maxLatency";
  private static final String MAX_WRITE_THREADS = "maxWriteThreads";
  protected static final long MAX_MEM_DEFAULT = 10_000_000L;
  protected static final long MAX_LATENCY_DEFAULT = 1_000L;
  protected static final int MAX_WRITE_THREADS_DEFAULT = 20;

  public static AccumuloConnectionConfig extractConnectionConfiguration(Configuration conf) {
    AccumuloConnectionConfig config = new AccumuloConnectionConfig();
    config.instance = conf.getString(INSTANCE);
    config.zooServers = conf.getString(ZOO_SERVERS);
    config.user = conf.getString(USER);
    config.key = conf.getString(KEY);
    config.maxMem = conf.getLong(MAX_MEM, MAX_MEM_DEFAULT);
    config.maxLatency = conf.getLong(MAX_LATENCY, MAX_LATENCY_DEFAULT);
    config.maxWriteThreads = conf.getInt(MAX_WRITE_THREADS, MAX_WRITE_THREADS_DEFAULT);
    return config;
  }
}
