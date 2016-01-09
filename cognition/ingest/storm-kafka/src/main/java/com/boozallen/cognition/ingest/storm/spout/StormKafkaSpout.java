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

package com.boozallen.cognition.ingest.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.boozallen.cognition.ingest.storm.Configurable;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Map;

/**
 * Encapsulate storm-kafka spout implementation (included as of 0.9.2) to take an apache commons Configuration.
 *
 * @author michaelkorb
 */
public class StormKafkaSpout extends BaseRichSpout implements Configurable {


  public static final String ZOOKEEPER = "stormKafkaConfig.kafkaZookeeper";
  public static final String ZK_ROOT = "stormKafkaConfig.zkRoot";
  public static final String BROKER_PATH = "stormKafkaConfig.brokerPath";
  public static final String TOPIC = "topic";
  public static final String SPOUT_ID = "spoutId"; //spouts need unique IDs so zookeeper can store kafka offsets
  public static final String FORCE_FROM_START = "forceFromStart";
  public static final String PERMITS_PER_SECOND = "permitsPerSecond";

  static final boolean DEFAULT_FORCE_FROM_START = false;
  static final Double DEFAULT_PERMITS_PER_SECOND = Double.NEGATIVE_INFINITY;

  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  RateLimiter rateLimiter;
  double permitsPerSecond; //number of tuples a spout task is allowed to send per second

  transient KafkaSpout kafkaSpout;
  SpoutConfig spoutConfig;

  @Override
  public void configure(Configuration conf) {
    spoutConfig = getSpoutConfig(conf);
    permitsPerSecond = conf.getDouble(PERMITS_PER_SECOND, DEFAULT_PERMITS_PER_SECOND);
  }

  SpoutConfig getSpoutConfig(Configuration conf) {
    String zookeeper = conf.getString(ZOOKEEPER);
    String brokerPath = conf.getString(BROKER_PATH);
    String topic = conf.getString(TOPIC);
    String zkRoot = conf.getString(ZK_ROOT);
    String spoutId = conf.getString(SPOUT_ID);
    boolean forceFromStart = conf.getBoolean(FORCE_FROM_START, DEFAULT_FORCE_FROM_START);

    logger.debug("Zookeeper: {}", zookeeper);
    logger.debug("BrokerPath: {}", brokerPath);
    logger.debug("Topic: {}", topic);
    logger.debug("ZkRoot: {}", zkRoot);
    logger.debug("Force From Start: {}", forceFromStart);

    ZkHosts zkHosts;
    if (StringUtils.isBlank(brokerPath)) {
      zkHosts = new ZkHosts(zookeeper);
    } else {
      zkHosts = new ZkHosts(zookeeper, brokerPath);
    }
    SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, zkRoot, spoutId);
    spoutConfig.forceFromStart = forceFromStart;
    return spoutConfig;
  }

  public boolean isRateLimited() {
    return Double.isFinite(permitsPerSecond);
  }

  @Override
  public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
    setupKafkaSpout();

    kafkaSpout.open(conf, context, collector);
    if (isRateLimited()) {
      rateLimiter = RateLimiter.create(permitsPerSecond);
    }
  }

  void setupKafkaSpout() {
    kafkaSpout = new KafkaSpout(spoutConfig);
  }

  @Override
  public void close() {
    kafkaSpout.close();
  }

  /**
   * Override nextTuple to implement rate limiting
   */
  @Override
  public void nextTuple() {
    if (isRateLimited()) {
      rateLimiter.acquire();
    }
    kafkaSpout.nextTuple();
  }

  @Override
  public void ack(Object msgId) {
    kafkaSpout.ack(msgId);
  }

  @Override
  public void fail(Object msgId) {
    kafkaSpout.fail(msgId);
  }

  @Override
  public void activate() {
    kafkaSpout.activate();
  }

  @Override
  public void deactivate() {
    kafkaSpout.deactivate();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(spoutConfig.scheme.getOutputFields());
  }
}