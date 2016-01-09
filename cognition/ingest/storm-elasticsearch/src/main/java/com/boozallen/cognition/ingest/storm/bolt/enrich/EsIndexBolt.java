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

package com.boozallen.cognition.ingest.storm.bolt.enrich;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.boozallen.cognition.ingest.storm.Configurable;
import org.apache.commons.configuration.Configuration;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.storm.EsBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * An adapter for {@link EsBolt} from Elasticsearch-Hadoop project. Designed to work with {@link ElasticSearchJsonBolt}.
 * Enables configuration via Cognition storm XML.
 * <p>
 * <pre>
 * <table>
 *   <tr>
 *     <td>subscribeToBolt</td>
 *     <td>should be set to ElasticSearchJsonBolt bolt number</td>
 *   </tr>
 *   <tr>
 *     <td>streamId</td>
 *   <td>set to <code>es_json</code>, which is secondary stream from ElasticSearchJsonBolt containing JSON for
 * indexing</td>
 *   </tr>
 *   <tr>
 *     <td>manualFlushFreqSecs</td>
 *     <td>force flush frequency using tick tuple (builtin EsBolt function)</td>
 *   </tr>
 *   <tr>
 *     <td>es.*</td>
 *     <td>configuration passed to EsBolt</td>
 *   </tr>
 * </table>
 * </pre>
 * <p>
 * Example configuration:
 * <pre>
 * {@code
 *
 * <conf>
 *   <subscribeToBolt>9</subscribeToBolt>
 *   <streamId>es_json</streamId>
 *   <manualFlushFreqSecs>5</manualFlushFreqSecs>
 *   <es.resource.write>{index}/{cognition_dataType}</es.resource.write>
 *   <es.storm.bolt.write.ack>true</es.storm.bolt.write.ack>
 *   <es.batch.size.bytes>100mb</es.batch.size.bytes>
 *   <es.mapping.id>sha1_checksum</es.mapping.id>
 *   <es.mapping.timestamp>postedTime</es.mapping.timestamp>
 *   <es.input.json>true</es.input.json>
 *   <es.nodes>ELASTICSEARCH-NODE-0,ELASTICSEARCH-NODE-1,ELASTICSEARCH-NODE-2</es.nodes>
 * </conf>
 * }</pre>
 */
public class EsIndexBolt implements IRichBolt, Configurable {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  transient EsBolt esBolt;
  private Map<String, String> boltConfig = new HashMap<>();

  static final String MANUAL_FLUSH_FREQ_SECS = "manualFlushFreqSecs";
  private int manualFlushFreqSecs;

  @Override
  public void configure(Configuration conf) {
    conf.getKeys().forEachRemaining(
        key -> {
          String keyString = key.toString();
          if (keyString.startsWith("es")) {
            String cleanedKey = keyString.replaceAll("\\.\\.", ".");
            boltConfig.put(cleanedKey, conf.getString(keyString));
          }
        });

    manualFlushFreqSecs = conf.getInt(MANUAL_FLUSH_FREQ_SECS);
  }

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    String target = boltConfig.get(ConfigurationOptions.ES_RESOURCE_WRITE);
    esBolt = new EsBolt(target, boltConfig);
    esBolt.prepare(conf, context, collector);
  }

  @Override
  public void execute(Tuple input) {
    try {
      esBolt.execute(input);
    } catch (Exception e) {
      logger.error("Index failed with tuple {}", input);
      logger.error("Index failed", e);
      throw new FailedException(e);
    }
  }

  @Override
  public void cleanup() {
    esBolt.cleanup();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Config conf = new Config();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, manualFlushFreqSecs);
    return conf;
  }

}