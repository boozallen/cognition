# Cognition Storm Ingest Example

This example demonstrates how Cognition libraries can be included in your own project.

## Dependencies

Depending on the necessary bolts and spouts for your project, you may include `storm-core` and other modules. All dependencies should be packaged in a uber jar for Storm deployment.

### Submitting Topology

Similar to standard Storm Topologies can be submitted using storm command line interface:

    storm jar uber.jar com.boozallen.cognition.ingest.storm.topology.ConfigurableIngestTopology topology.xml

## Example Topologies

### TwitterTopology.xml

Example topology pulling tweet entries from Kafka, enrich and stores result in Elasticsearch and Accumulo
 
### BlueCoatTopology.xml

Example topology pulling bluecoat proxy log entries from Kafka, parse and stores result in Accumulo
 
