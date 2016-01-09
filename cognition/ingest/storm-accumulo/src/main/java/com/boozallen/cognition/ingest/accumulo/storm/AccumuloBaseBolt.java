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

package com.boozallen.cognition.ingest.accumulo.storm;

import backtype.storm.task.TopologyContext;
import com.boozallen.cognition.ingest.accumulo.utils.AccumuloConnectionConfig;
import com.boozallen.cognition.ingest.accumulo.utils.AccumuloConnectionUtils;
import com.boozallen.cognition.ingest.storm.PrepareFailedException;
import com.boozallen.cognition.ingest.storm.bolt.AbstractProcessingBolt;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base bolt for writing to accumulo. Manages accumulo configuration and connection.
 */
public abstract class AccumuloBaseBolt extends AbstractProcessingBolt {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  protected AccumuloConnectionConfig accumuloConnConfig;
  protected BatchWriterConfig config;
  protected Connector conn;

  @Override
  public final void configure(Configuration conf) {
    accumuloConnConfig = AccumuloConnectionUtils.extractConnectionConfiguration(conf);
    configureAccumuloBolt(conf);
  }

  abstract void configureAccumuloBolt(Configuration conf);

  @Override
  public final void prepare(Map stormConf, TopologyContext context) {
    Instance inst = new ZooKeeperInstance(accumuloConnConfig.getInstance(), accumuloConnConfig.getZooServers());
    try {
      conn = inst.getConnector(accumuloConnConfig.getUser(), new PasswordToken(accumuloConnConfig.getKey()));
      config = new BatchWriterConfig();
      config.setMaxMemory(accumuloConnConfig.getMaxMem());
      config.setMaxLatency(accumuloConnConfig.getMaxLatency(), TimeUnit.MILLISECONDS);
      config.setMaxWriteThreads(accumuloConnConfig.getMaxWriteThreads());
    } catch (AccumuloException e) {
      logger.error("Failed to get Connection", e);
      throw new PrepareFailedException("Failed to get Connection", e);
    } catch (AccumuloSecurityException e) {
      logger.error("Failed to authenticate", e);
      throw new PrepareFailedException("Failed to authenticate", e);
    }
    prepareAccumuloBolt(stormConf, context);
  }

  abstract void prepareAccumuloBolt(Map stormConf, TopologyContext context);
}
