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

package com.boozallen.cognition.ingest.storm.topology;

import backtype.storm.*;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.*;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.boozallen.cognition.ingest.storm.Configurable;
import com.boozallen.cognition.ingest.storm.ConfigurationException;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

import static com.boozallen.cognition.ingest.storm.topology.ConfigurableIngestTopologyConstants.*;

/**
 *
 */
public class ConfigurableIngestTopology {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  /**
   * This class takes an XML configuration and builds and submits a storm topology.
   *
   * @throws org.apache.commons.configuration.ConfigurationException
   * @throws ConfigurationException
   */
  public static void main(String[] args) throws Exception {
    // using config file
    if (args.length != 1) {
      System.err.println("usage: xmlConfigFile");
      System.exit(1);
    }
    XMLConfiguration conf = new XMLConfiguration(args[0]);

    ConfigurableIngestTopology topology = new ConfigurableIngestTopology();
    topology.configure(conf);

    boolean doLocalSubmit = conf.getBoolean(DO_LOCAL_SUBMIT, DO_LOCAL_SUBMIT_DEFAULT);
    if (doLocalSubmit)
      topology.submitLocal(new LocalCluster());
    else
      topology.submit();
  }

  String topologyName;
  Config stormConfig;
  TopologyBuilder builder;

  void configure(HierarchicalConfiguration conf) throws Exception {

    this.stormConfig = new Config();
    this.builder = new TopologyBuilder();

    SubnodeConfiguration topologyConf = conf.configurationAt(TOPOLOGY);
    this.topologyName = getTopologyName(topologyConf);

    SubnodeConfiguration stormConf = topologyConf.configurationAt(STORM_CONF);
    configureStorm(stormConf, stormConfig);

    SubnodeConfiguration spoutConf = conf.configurationAt(SPOUT);
    String spout = configureSpout(builder, spoutConf);

    SubnodeConfiguration boltConfs = conf.configurationAt(BOLTS);
    configureBolts(builder, boltConfs, spout);
  }

  void submitLocal(ILocalCluster cluster) throws AlreadyAliveException, InvalidTopologyException {
    stormConfig.setDebug(true);
    cluster.submitTopology(topologyName, stormConfig, builder.createTopology());
  }

  void submit() throws AlreadyAliveException, InvalidTopologyException {
    StormSubmitter.submitTopology(topologyName, stormConfig, builder.createTopology());
  }

  String getTopologyName(Configuration topologyConf) {
    String topologyName = topologyConf.getString(TOPOLOGY_NAME);
    Validate.notBlank(topologyName, "Missing topology name");
    return topologyName;
  }

  protected void configureStorm(Configuration conf, Config stormConf) throws IllegalAccessException {
    stormConf.registerSerialization(LogRecord.class);
    //stormConf.registerSerialization(Entity.class);
    stormConf.registerMetricsConsumer(LoggingMetricsConsumer.class);

    for (Iterator<String> iter = conf.getKeys(); iter.hasNext(); ) {
      String key = iter.next();

      String keyString = key.toString();
      String cleanedKey = keyString.replaceAll("\\.\\.", ".");

      String schemaFieldName = cleanedKey.replaceAll("\\.", "_").toUpperCase() + "_SCHEMA";
      Field field = FieldUtils.getField(Config.class, schemaFieldName);
      Object fieldObject = field.get(null);

      if (fieldObject == Boolean.class)
        stormConf.put(cleanedKey, conf.getBoolean(keyString));
      else if (fieldObject == String.class)
        stormConf.put(cleanedKey, conf.getString(keyString));
      else if (fieldObject == ConfigValidation.DoubleValidator)
        stormConf.put(cleanedKey, conf.getDouble(keyString));
      else if (fieldObject == ConfigValidation.IntegerValidator)
        stormConf.put(cleanedKey, conf.getInt(keyString));
      else if (fieldObject == ConfigValidation.PowerOf2Validator)
        stormConf.put(cleanedKey, conf.getLong(keyString));
      else if (fieldObject == ConfigValidation.StringOrStringListValidator)
        stormConf.put(cleanedKey, Arrays.asList(conf.getStringArray(keyString)));
      else if (fieldObject == ConfigValidation.StringsValidator)
        stormConf.put(cleanedKey, Arrays.asList(conf.getStringArray(keyString)));
      else {
        logger.error("{} cannot be configured from XML. Consider configuring in navie storm configuration.");
        throw new UnsupportedOperationException(cleanedKey + " cannot be configured from XML");
      }
    }
  }

  StormParallelismConfig getStormParallelismConfig(Configuration conf) {
    return new StormParallelismConfig(conf);
  }

  /**
   * Gets a topology builder and a storm spout configuration. Initializes the spout and sets it with the topology
   * builder.
   *
   * @param builder
   * @param spout
   * @return
   * @throws ConfigurationException
   */
  String configureSpout(TopologyBuilder builder, Configuration spout)
      throws ConfigurationException {
    String spoutType = spout.getString(TYPE);
    Configuration spoutConf = spout.subset(CONF);
    StormParallelismConfig parallelismConfig = getStormParallelismConfig(spoutConf);

    IRichSpout spoutComponent = (IRichSpout) buildComponent(spoutType, spoutConf);

    builder
        .setSpout(spoutType, spoutComponent, parallelismConfig.getParallelismHint())
        .setNumTasks(parallelismConfig.getNumTasks());
    return spoutType;
  }

  /**
   * Gets a topology builder and set of storm bolt configurations. Initializes the bolts and sets them with the
   * topology builder.
   *
   * @param builder   storm topology builder
   * @param boltsConf component configurations
   * @param spout     type of storm component being added (spout/bolt)
   * @throws ConfigurationException
   */
  void configureBolts(TopologyBuilder builder, SubnodeConfiguration boltsConf, String spout)
      throws ConfigurationException {

    String prevComponent = spout;
    // bolts subscribe to other bolts by number
    HashMap<Integer, String> boltNumberToId = new HashMap<>();

    List<HierarchicalConfiguration> bolts = boltsConf.configurationsAt(BOLT);
    for (HierarchicalConfiguration bolt : bolts) {
      String boltType = bolt.getString(TYPE);
      Configuration boltConf = bolt.configurationAt(CONF);
      int boltNum = bolt.getInt(NUMBER_ATTRIBUTE);

      String boltId = String.format("%02d_%s", boltNum, boltType);
      boltNumberToId.put(boltNum, boltId);
      String subscribingToComponent = getSubscribingToComponent(prevComponent, boltNumberToId, boltConf);
      logger.info("{} subscribing to {}", boltId, subscribingToComponent);

      IComponent baseComponent = buildComponent(boltType, boltConf);

      StormParallelismConfig stormParallelismConfig = getStormParallelismConfig(boltConf);
      BoltDeclarer declarer = buildBoltDeclarer(builder, stormParallelismConfig, boltId, baseComponent);

      configureTickFrequency(boltConf, declarer);

      configureStreamGrouping(subscribingToComponent, boltConf, declarer);

      prevComponent = boltId;

      boltNum++;
    }
  }

  String getSubscribingToComponent(
      String prevComponent, Map<Integer, String> boltNumberToId, Configuration boltConf) {

    int subscribingToBoltNumber = boltConf.getInt(SUBSCRIBE_TO_BOLT, -1);
    boolean isSubscribeToBoltDeclared = subscribingToBoltNumber >= 0;

    if (isSubscribeToBoltDeclared && boltNumberToId.containsKey(subscribingToBoltNumber))
      return boltNumberToId.get(subscribingToBoltNumber);
    else
      return prevComponent;
  }

  BoltDeclarer buildBoltDeclarer(TopologyBuilder builder,
                                 StormParallelismConfig stormParallelismConfig,
                                 String boltId,
                                 IComponent baseComponent) throws ConfigurationException {

    if (baseComponent instanceof IBasicBolt) {
      return builder
          .setBolt(boltId, (IBasicBolt) baseComponent, stormParallelismConfig.getParallelismHint())
          .setNumTasks(stormParallelismConfig.getNumTasks());
    } else if (baseComponent instanceof IRichBolt) {
      return builder
          .setBolt(boltId, (IRichBolt) baseComponent, stormParallelismConfig.getParallelismHint())
          .setNumTasks(stormParallelismConfig.getNumTasks());
    } else {
      throw new ConfigurationException("Unknown component type: " + baseComponent.getClass());
    }
  }

  void configureTickFrequency(Configuration boltConf, BoltDeclarer boltDeclarer) {
    String tickFrequency = boltConf.getString(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS.replaceAll("\\.", ".."));
    if (StringUtils.isNotBlank(tickFrequency)) {
      boltDeclarer.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequency);
    }
  }

  void configureStreamGrouping(String prevComponent, Configuration boltConf, BoltDeclarer declarer)
      throws ConfigurationException {

    String streamType = boltConf.getString(STREAM_GROUPING_CONF_TYPE, STREAM_GROUPING_LOCAL_OR_SHUFFLE);
    String streamId = boltConf.getString(STREAM_ID, Utils.DEFAULT_STREAM_ID);

    if (StringUtils.equals(streamType, STREAM_GROUPING_FIELDS)) {
      configureStreamFieldsGrouping(prevComponent, streamId, boltConf, declarer);
    } else if (StringUtils.equals(streamType, STREAM_GROUPING_LOCAL_OR_SHUFFLE)) {
      declarer.localOrShuffleGrouping(prevComponent, streamId);
    } else {
      declarer.shuffleGrouping(prevComponent, streamId);
    }
  }

  void configureStreamFieldsGrouping(String prevComponent, String streamId, Configuration boltConf, BoltDeclarer declarer)
      throws ConfigurationException {

    String streamTypeArgs = boltConf.getString(STREAM_GROUPING_CONF_ARGS);

    if (StringUtils.isBlank(streamTypeArgs)) {
      logger.error("{} stream grouping requires {} configuration",
          STREAM_GROUPING_FIELDS, STREAM_GROUPING_CONF_ARGS);
      throw new ConfigurationException("Missing stream grouping configuration");
    } else {
      String[] fields = streamTypeArgs.split(",");
      declarer.fieldsGrouping(prevComponent, streamId, new Fields(fields));
    }
  }

  /**
   * Takes a storm component type string and a configuration for that component, and returns the specified component
   * constructed with the configuration.
   *
   * @param componentClassName
   * @param conf
   * @return
   * @throws ConfigurationException
   */
  IComponent buildComponent(String componentClassName, Configuration conf)
      throws ConfigurationException {
    try {
      Class component = Class.forName(componentClassName);
      IComponent instance = (IComponent) component.newInstance();
      if (instance instanceof Configurable) {
        Configurable configurable = (Configurable) instance;
        configurable.configure(conf);
      }
      return instance;

    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      logger.error("Unable to instantiate component: {}", componentClassName);
      throw new ConfigurationException(e);
    }
  }

}
