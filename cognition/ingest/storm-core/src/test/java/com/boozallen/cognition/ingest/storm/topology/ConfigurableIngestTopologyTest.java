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

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.*;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.boozallen.cognition.ingest.storm.ConfigurationException;
import com.boozallen.cognition.test.utils.TestResourceUtils;
import mockit.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static backtype.storm.Config.*;
import static com.boozallen.cognition.ingest.storm.topology.ConfigurableIngestTopologyConstants.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class ConfigurableIngestTopologyTest {
  @Tested
  ConfigurableIngestTopology topology;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConfigure(
      @Injectable HierarchicalConfiguration conf,
      @Injectable SubnodeConfiguration topologyConf,
      @Injectable SubnodeConfiguration stormConf,
      @Injectable SubnodeConfiguration spoutConf,
      @Injectable SubnodeConfiguration boltConfs,
      @Injectable String topologyName,
      @Injectable String spout) throws Exception {

    new Expectations(topology) {{
      conf.configurationAt(TOPOLOGY);
      result = topologyConf;
      topology.getTopologyName(topologyConf);
      result = topologyName;

      topologyConf.configurationAt(STORM_CONF);
      result = stormConf;
      topology.configureStorm(stormConf, (Config) any);

      conf.configurationAt(SPOUT);
      result = spoutConf;
      topology.configureSpout((TopologyBuilder) any, spoutConf);
      result = spout;

      conf.configurationAt(BOLTS);
      result = boltConfs;
      topology.configureBolts((TopologyBuilder) any, boltConfs, spout);
    }};
    topology.configure(conf);
  }

  @Test
  public void testSubmitLocal(
      @Injectable String topologyName,
      @Injectable Config stormConfig,
      @Injectable TopologyBuilder builder,
      @Injectable ILocalCluster cluster,
      @Injectable StormTopology stormTopology
  ) throws Exception {
    topology.topologyName = topologyName;
    topology.stormConfig = stormConfig;
    topology.builder = builder;

    new Expectations() {{
      builder.createTopology();
      result = stormTopology;
      cluster.submitTopology(topologyName, stormConfig, stormTopology);
    }};

    topology.submitLocal(cluster);
  }

  @Test
  public void testSubmit(
      @Injectable String topologyName,
      @Injectable Config stormConfig,
      @Injectable TopologyBuilder builder,
      @Injectable StormTopology stormTopology
  ) throws Exception {
    topology.topologyName = topologyName;
    topology.stormConfig = stormConfig;
    topology.builder = builder;

    new MockUp<StormSubmitter>() {
      @Mock
      void submitTopology(String name, Map stormConf, StormTopology topology) {
      }
    };

    new Expectations() {{
      builder.createTopology();
      result = stormTopology;
    }};

    topology.submit();
  }

  @Test
  public void testGetTopologyName() throws Exception {
    URL resource = TestResourceUtils.getResource(this.getClass(), "topology-config.xml");
    XMLConfiguration conf = new XMLConfiguration(resource);

    assertThat(topology.getTopologyName(conf), is("TestTopology"));
  }

  @Test
  public void testConfigureStorm() throws Exception {
    URL resource = TestResourceUtils.getResource(this.getClass(), "storm-config.xml");
    XMLConfiguration conf = new XMLConfiguration(resource);
    Config stormConf = new Config();

    topology.configureStorm(conf, stormConf);

    assertThat(stormConf.get(TOPOLOGY_WORKERS), is(2));
    assertThat(stormConf.get(TOPOLOGY_MESSAGE_TIMEOUT_SECS), is(300));
    assertThat(stormConf.get(TOPOLOGY_MAX_SPOUT_PENDING), is(10));
    assertThat(stormConf.get(TOPOLOGY_WORKER_CHILDOPTS), is(Arrays.asList("-Xmx4G")));
    assertThat(stormConf.get(TOPOLOGY_DEBUG), is(true));
    assertThat(stormConf.get(TOPOLOGY_STATS_SAMPLE_RATE), is(2.0d));
    assertThat(stormConf.get(TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), is(16L));
    assertThat(stormConf.get(STORM_ZOOKEEPER_SERVERS), is(Arrays.asList("host0:2081", "host1:2081")));
    assertThat(stormConf.get(STORM_LOCAL_DIR), is("/storm-local"));
  }

  @Test
  public void testConfigureStormUnsupported(@Injectable Configuration conf) throws Exception {
    List<String> keys = Arrays.asList("topology..metrics..consumer..register");
    new Expectations() {{
      conf.getKeys();
      result = keys.iterator();

    }};

    Config stormConf = new Config();
    thrown.expect(UnsupportedOperationException.class);
    topology.configureStorm(conf, stormConf);
  }

  @Test
  public void testGetStormParallelismConfig(@Injectable Configuration conf) throws Exception {
    int parallelismHint = 1;
    new Expectations() {{
      conf.getInt(PARALLELISM_HINT, PARALLELISM_HINT_DEFAULT);
      result = parallelismHint;
      conf.getInt(NUM_TASKS, parallelismHint);
      result = parallelismHint;
    }};

    StormParallelismConfig parallelismConfig = topology.getStormParallelismConfig(conf);

    assertThat(parallelismConfig.getParallelismHint(), is(parallelismHint));
    assertThat(parallelismConfig.getNumTasks(), is(parallelismHint));
  }

  @Test
  public void testConfigureSpout(
      @Injectable TopologyBuilder builder,
      @Injectable Configuration spout,
      @Injectable Configuration spoutConf,
      @Injectable StormParallelismConfig stormParallelismConfig,
      @Injectable IRichSpout spoutComponent) throws Exception {

    String spoutType = "spout-class";

    new Expectations(topology) {{
      spout.getString(TYPE);
      result = spoutType;
      spout.subset(CONF);
      result = spoutConf;
      topology.getStormParallelismConfig(spoutConf);
      result = stormParallelismConfig;
      topology.buildComponent(spoutType, spoutConf);
      result = spoutComponent;
    }};

    assertThat(topology.configureSpout(builder, spout), is(spoutType));
  }

  @Test
  public void testConfigureBolts(
      @Injectable TopologyBuilder builder,
      @Injectable SubnodeConfiguration boltsConf,
      @Injectable HierarchicalConfiguration bolt,
      @Injectable SubnodeConfiguration boltConf,
      @Injectable IComponent baseComponent,
      @Injectable StormParallelismConfig stormParallelismConfig,
      @Injectable BoltDeclarer declarer) throws Exception {

    List<HierarchicalConfiguration> bolts = Arrays.asList(bolt);
    String spout = "spout_id";

    new Expectations(topology) {{
      boltsConf.configurationsAt(BOLT);
      result = bolts;

      bolt.getString(TYPE);
      result = "bolt_type";
      bolt.configurationAt(CONF);
      result = boltConf;
      bolt.getInt(NUMBER_ATTRIBUTE);
      result = 0;
      topology.getSubscribingToComponent(spout, (HashMap<Integer, String>) any, boltConf);
      result = spout;

      topology.buildComponent("bolt_type", boltConf);
      result = baseComponent;

      topology.getStormParallelismConfig(boltConf);
      result = stormParallelismConfig;
      topology.buildBoltDeclarer(builder, stormParallelismConfig, "00_bolt_type", baseComponent);
      result = declarer;

      topology.configureTickFrequency(boltConf, declarer);

      topology.configureStreamGrouping(spout, boltConf, declarer);

    }};

    topology.configureBolts(builder, boltsConf, spout);
  }

  @Test
  public void testGetSubscribingToComponent0(
      @Injectable String prevComponent,
      @Injectable Map<Integer, String> boltNumberToId,
      @Injectable Configuration boltConf) {
    new Expectations() {{
      boltConf.getInt(SUBSCRIBE_TO_BOLT, -1);
      result = -1;
    }};

    String result = topology.getSubscribingToComponent(prevComponent, boltNumberToId, boltConf);
    assertThat(result, is(prevComponent));
  }

  @Test
  public void testGetSubscribingToComponent1(
      @Injectable String prevComponent,
      @Injectable String subscribedBolt,
      @Injectable Map<Integer, String> boltNumberToId,
      @Injectable Configuration boltConf) {
    int subscribedBoltNumber = 0;

    new Expectations() {{
      boltConf.getInt(SUBSCRIBE_TO_BOLT, -1);
      result = subscribedBoltNumber;
      boltNumberToId.containsKey(subscribedBoltNumber);
      result = true;
      boltNumberToId.get(subscribedBoltNumber);
      result = subscribedBolt;
    }};

    String result = topology.getSubscribingToComponent(prevComponent, boltNumberToId, boltConf);
    assertThat(result, is(subscribedBolt));
  }

  @Test
  public void testGetSubscribingToComponent2(
      @Injectable String prevComponent,
      @Injectable Map<Integer, String> boltNumberToId,
      @Injectable Configuration boltConf) {
    int subscribedBoltNumber = 0;

    new Expectations() {{
      boltConf.getInt(SUBSCRIBE_TO_BOLT, -1);
      result = subscribedBoltNumber;
      boltNumberToId.containsKey(subscribedBoltNumber);
      result = false;
    }};

    String result = topology.getSubscribingToComponent(prevComponent, boltNumberToId, boltConf);
    assertThat(result, is(prevComponent));
  }

  @Test
  public void testBuildBoltDeclarerIBasicBolt(
      @Injectable TopologyBuilder builder,
      @Injectable StormParallelismConfig stormParallelismConfig,
      @Injectable String boltId,
      @Injectable IBasicBolt baseComponent,
      @Injectable BoltDeclarer boltDeclarer) throws Exception {
    new Expectations() {{
      stormParallelismConfig.getParallelismHint();
      result = 1;
      stormParallelismConfig.getNumTasks();
      result = 1;

      builder.setBolt(boltId, baseComponent, 1);
      result = boltDeclarer;
      boltDeclarer.setNumTasks(1);
    }};

    topology.buildBoltDeclarer(builder, stormParallelismConfig, boltId, baseComponent);
  }

  @Test
  public void testBuildBoltDeclarerIRichBolt(
      @Injectable TopologyBuilder builder,
      @Injectable StormParallelismConfig stormParallelismConfig,
      @Injectable String boltId,
      @Injectable IRichBolt baseComponent,
      @Injectable BoltDeclarer boltDeclarer) throws Exception {
    new Expectations() {{
      stormParallelismConfig.getParallelismHint();
      result = 1;
      stormParallelismConfig.getNumTasks();
      result = 1;

      builder.setBolt(boltId, baseComponent, 1);
      result = boltDeclarer;
      boltDeclarer.setNumTasks(1);
    }};

    topology.buildBoltDeclarer(builder, stormParallelismConfig, boltId, baseComponent);
  }

  @Test
  public void testBuildBoltDeclarerUnknownComponent(
      @Injectable TopologyBuilder builder,
      @Injectable StormParallelismConfig stormParallelismConfig,
      @Injectable String boltId,
      @Injectable TestClassUnknownComponent baseComponent) throws Exception {

    thrown.expect(ConfigurationException.class);
    topology.buildBoltDeclarer(builder, stormParallelismConfig, boltId, baseComponent);
  }

  @Test
  public void testConfigureTickFrequency(
      @Injectable BoltDeclarer boltDeclarer) throws Exception {
    URL resource = TestResourceUtils.getResource(this.getClass(), "tick-frequency-config.xml");
    XMLConfiguration conf = new XMLConfiguration(resource);
    new Expectations() {{
      boltDeclarer.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, "30");
    }};
    topology.configureTickFrequency(conf, boltDeclarer);
  }

  @Test
  public void testConfigureTickFrequencyEmpty(
      @Injectable Configuration conf,
      @Injectable BoltDeclarer boltDeclarer) throws Exception {
    new Expectations() {{
      conf.getString(anyString);
      result = "";
      boltDeclarer.addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, any);
      times = 0;
    }};
    topology.configureTickFrequency(conf, boltDeclarer);
  }

  @Test
  public void testConfigureStreamGrouping_StreamFieldsGrouping(
      @Injectable String prevComponent,
      @Injectable Configuration boltConf,
      @Injectable BoltDeclarer declarer) throws Exception {
    new Expectations(topology) {{
      boltConf.getString(STREAM_GROUPING_CONF_TYPE, STREAM_GROUPING_LOCAL_OR_SHUFFLE);
      result = STREAM_GROUPING_FIELDS;
      boltConf.getString(STREAM_ID, Utils.DEFAULT_STREAM_ID);
      result = Utils.DEFAULT_STREAM_ID;
      topology.configureStreamFieldsGrouping(prevComponent, Utils.DEFAULT_STREAM_ID, boltConf, declarer);
    }};

    topology.configureStreamGrouping(prevComponent, boltConf, declarer);
  }

  @Test
  public void testConfigureStreamGrouping_localOrShuffleGrouping(
      @Injectable String prevComponent,
      @Injectable Configuration boltConf,
      @Injectable BoltDeclarer declarer) throws Exception {
    new Expectations(topology) {{
      boltConf.getString(STREAM_GROUPING_CONF_TYPE, STREAM_GROUPING_LOCAL_OR_SHUFFLE);
      result = STREAM_GROUPING_LOCAL_OR_SHUFFLE;
      boltConf.getString(STREAM_ID, Utils.DEFAULT_STREAM_ID);
      result = Utils.DEFAULT_STREAM_ID;
      declarer.localOrShuffleGrouping(prevComponent, Utils.DEFAULT_STREAM_ID);
    }};

    topology.configureStreamGrouping(prevComponent, boltConf, declarer);
  }

  @Test
  public void testConfigureStreamGrouping_default(
      @Injectable String prevComponent,
      @Injectable Configuration boltConf,
      @Injectable BoltDeclarer declarer) throws Exception {
    new Expectations(topology) {{
      boltConf.getString(STREAM_GROUPING_CONF_TYPE, STREAM_GROUPING_LOCAL_OR_SHUFFLE);
      result = STREAM_GROUPING_SHUFFLE;
      boltConf.getString(STREAM_ID, Utils.DEFAULT_STREAM_ID);
      result = Utils.DEFAULT_STREAM_ID;
      declarer.shuffleGrouping(prevComponent, Utils.DEFAULT_STREAM_ID);
    }};

    topology.configureStreamGrouping(prevComponent, boltConf, declarer);
  }

  @Test
  public void testConfigureStreamFieldsGrouping(
      @Injectable String prevComponent,
      @Injectable String streamId,
      @Injectable Configuration boltConf,
      @Injectable BoltDeclarer declarer,
      @Injectable Fields fields) throws Exception {
    new Expectations(Fields.class) {{
      boltConf.getString(STREAM_GROUPING_CONF_ARGS);
      result = "a,b";
      new Fields(new String[]{"a", "b"});
      result = fields;
      declarer.fieldsGrouping(prevComponent, streamId, fields);
    }};

    topology.configureStreamFieldsGrouping(prevComponent, streamId, boltConf, declarer);
  }

  @Test
  public void configureStreamFieldsGroupingFail(
      @Injectable String prevComponent,
      @Injectable String streamId,
      @Injectable Configuration boltConf,
      @Injectable BoltDeclarer declarer) throws Exception {
    new Expectations() {{
      boltConf.getString(STREAM_GROUPING_CONF_ARGS);
      result = "";
    }};

    thrown.expect(ConfigurationException.class);
    topology.configureStreamFieldsGrouping(prevComponent, streamId, boltConf, declarer);
  }

  @Test
  public void testBuildComponent(
      @Injectable Configuration conf) throws Exception {
    new Expectations(TestClassIComponent.class) {{
      new TestClassIComponent();
    }};

    assertNotNull(topology.buildComponent(TestClassIComponent.class.getName(), conf));
  }

  @Test
  public void testBuildComponentConfigurable(
      @Injectable Configuration conf,
      @Injectable TestClassConfigurableComponent component) throws Exception {

    new Expectations(TestClassConfigurableComponent.class) {{
      new TestClassConfigurableComponent();
      result = component;
      component.configure(conf);
    }};

    assertNotNull(topology.buildComponent(TestClassConfigurableComponent.class.getName(), conf));
  }

  @Test
  public void testBuildComponentException(@Injectable Configuration conf) throws Exception {
    thrown.expect(ConfigurationException.class);
    topology.buildComponent("InvalidClass", conf);
  }
}