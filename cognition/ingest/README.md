# Cognition Ingest Library

Cognition Ingest Library contains components that simplifies common data ingest workflow.

Included modules:

 - storm-core
    - Core module includes base components 
 - storm-accumulo
    - [Apache Accumulo](https://github.com/apache/accumulo) related components
 - storm-elasticsearch
    - [Elasticsearch](https://github.com/elastic/elasticsearch) related components
 - storm-geo
    - Geolocation related components
 - storm-kafka
    - [Apache Kafka](https://github.com/apache/kafka) related components

## Cognition Storm Ingest

Cognition created a custom XML schema on top of Apache Storm for defining topologies. The XML schema externalizes topology configuration, allowing topology modifications without re-compilation of deployment jar file. The aim of the XML schema is not to fully replicate Storm’s API in XML format, but rather creates a simplified abstraction for data ingestion using Cognition spouts and bolts.

The XML schema consists of three main sections – topology, spout, and bolts.

### Topology
`<topology>` section contains overall topology configuration.

#### Name
`<name>` contains topology name, used during topology submission. This field is required.

#### Storm Configuration
`<storm-conf>` contains storm configurations that would be passed to backtype.storm.Config

#### Example
    <topology>
      <name>TwitterTopology</name>
      <storm-conf>
        <topology.workers>3</topology.workers>
        <topology.message.timeout.secs>300</topology.message.timeout.secs>
        <topology.max.spout.pending>5000</topology.max.spout.pending>
      </storm-conf>
    </topology>

### Spout
`<spout>` section defines the single spout in the topology.

#### Type
`<type>` defines fully qualifies Spout class

#### Configuration
`<conf>` defines spout-specific configurations. Configuration is passed to spout via com.boozallen.cognition.ingest.storm.Configurable#configure.

#### Example

    <spout>
      <type>com.boozallen.cognition.ingest.storm.spout.StormKafkaSpout</type>
      <conf>
        <stormKafkaConfig>
          <kafkaZookeeper>KAFKA-ZOOKEEPER-0:2181,KAFKA-ZOOKEEPER-1:2181,KAFKA-ZOOKEEPER-2:2181</kafkaZookeeper>
          <brokerPath>/brokers</brokerPath>
          <zkRoot>/storm-kafka</zkRoot>
        </stormKafkaConfig>
        <spoutId>twitter-ingest</spoutId>
        <topic>gnip-twitter-records</topic>
        <parallelismHint>3</parallelismHint>
        <topology.debug>true</topology.debug>
      </conf>
    </spout>

### Bolts
`<bolts>` section defines bolt chain that would process the data. It contains a list of <bolt> definitions. Unless specifically defined, bolts are executed linearly, receiving emitted stream from previous bolt in chain, or from spout.

#### Bolt
`<bolt>` defines individual bolts for data processing. Each bolt must define a unique bolt number. Bolt numbers are referenced when defining non-linear topologies.

##### Type
`<type>` defines fully qualifies Bolt class

##### Configuration
`<conf>` defines bolt-specific configurations. Configuration is passed to bolt via com.boozallen.cognition.ingest.storm.Configurable#configure method.

##### Example

    <bolts>
      <bolt number="0">
        <type>com.boozallen.cognition.ingest.storm.bolt.starter.FlattenJsonBolt</type>
        <conf>
          <parallelismHint>3</parallelismHint>
        </conf>
      </bolt>
      <bolt number="1">
        <type>com.boozallen.cognition.ingest.storm.bolt.logic.SkipFieldValueBolt</type>
        <conf>
          <parallelismHint>3</parallelismHint>
          <field>verb</field>
          <value>delete</value>
        </conf>
      </bolt>
      <bolt number="2">
        <type>com.boozallen.cognition.ingest.storm.bolt.logic.SkipBlankBolt</type>
        <conf>
          <parallelismHint>3</parallelismHint>
          <field>postedTime</field>
        </conf>
      </bolt>
      <bolt number="3">
        <type>com.boozallen.cognition.ingest.storm.bolt.enrich.AddMetadataBolt</type>
        <conf>
          <parallelismHint>3</parallelismHint>
          <field>cognition.dataType</field>
          <value>twitter</value>
        </conf>
      </bolt>
    </bolts>

### Spout and Bolt Configurations

These general configurations are available in all spouts and bolts. Documentation for specific configuration for each spout and bolt can be found in their perspective javadoc.  

#### Parallelism Hint and Number of Tasks

`<parallelismHint>` and `<numTasks>` sets parallelism hint and number of tasks for each spout or bolt. Refer to [storm documentation on parallelism](http://storm.apache.org/documentation/Understanding-the-parallelism-of-a-Storm-topology.html).

##### Example

    <bolt number="0">
      <type>com.boozallen.cognition.ingest.storm.bolt.starter.FlattenJsonBolt</type>
      <conf>
        <parallelismHint>3</parallelismHint>
        <numTasks>3</numTasks>
        ...
      </conf>
    </bolt>

#### Stream Grouping
`<streamGrouping>` sets stream grouping of each bolt. Supported stream groupings includes: `Fields`, `LocalOrShuffle` and `Shuffle`. `Fields` grouping requires additional `<streamGroupingArgs>` configuration to include comma separated field names.
Refer to [storm documentation on stream groupings](http://storm.apache.org/documentation/Concepts.html#stream-groupings).

##### Example

    <bolt number="0">
      <type>com.boozallen.cognition.ingest.storm.bolt.starter.FlattenJsonBolt</type>
      <conf>
        <streamGrouping>Fields</streamGrouping>
        <streamGroupingArgs>field0,field1</streamGroupingArgs>
        ...
      </conf>
    </bolt>


    <bolt number="0">
      <type>com.boozallen.cognition.ingest.storm.bolt.starter.FlattenJsonBolt</type>
      <conf>
        <streamGrouping>LocalOrShuffle</streamGrouping>
        ...
      </conf>
    </bolt>


    <bolt number="0">
      <type>com.boozallen.cognition.ingest.storm.bolt.starter.FlattenJsonBolt</type>
      <conf>
        <streamGrouping>Shuffle</streamGrouping>
        ...
      </conf>
    </bolt>


### Advanced Topology Configuration
By default, each bolt receives emitted stream from previous bolt in chain, or from spout. In situations where this is not enough, custom configuration is possible.

`<subscribeToBolt>` defines the previous bolt number this bolt is subscribed to.

`<streamId>` optionally defines the stream the bolt should subscribe to.

#### Example

    <bolt number="10">
      <type>com.bah.pip.storm.bolt.EsIndexBolt</type>
      <conf>
        <subscribeToBolt>9</subscribeToBolt>
        <streamId>es_json</streamId>
        ...
      </conf>
    </bolt>

### Submitting Topology

Similar to standard Storm Topologies can be submitted using storm command line interface:

    storm jar uber.jar com.boozallen.cognition.ingest.storm.topology.ConfigurableIngestTopology topology.xml