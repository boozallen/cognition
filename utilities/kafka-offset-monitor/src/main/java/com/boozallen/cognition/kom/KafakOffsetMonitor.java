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

package com.boozallen.cognition.kom;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.cli.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author bentse
 */
public class KafakOffsetMonitor extends TimerTask {

  private static Options options = new Options();

  static {
    Option zkHosts = new Option("z", "zkHosts", true, "Zookeeper host string i.e. host1:port1,host2:port2");
    zkHosts.setRequired(true);
    Option zkRoot = new Option("r", "zkRoot", true, "Zookeeper root for spout states");
    Option spoutId = new Option("s", "spoutId", true, "Kafka Spout ID");
    spoutId.setRequired(true);
    Option logstashHost = new Option("h", "logstashHost", true, "Logstash host");
    logstashHost.setRequired(true);
    Option logstashPort = new Option("p", "logstashPort", true, "Logstash TCP input port");
    logstashPort.setRequired(true);
    Option refresh = new Option("f", "refresh", true, "Refresg rate, in seconds");

    options.addOption(zkHosts);
    options.addOption(zkRoot);
    options.addOption(spoutId);
    options.addOption(logstashHost);
    options.addOption(logstashPort);
    options.addOption(refresh);
  }


  public static void main(String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("KafakOffsetMonitor", options);
      System.exit(1);
    }

    KafakOffsetMonitor monitor = new KafakOffsetMonitor();
    monitor.zkHosts = cmd.getOptionValue("zkHosts");
    monitor.zkRoot = cmd.getOptionValue("zkRoot", "/storm-kafka");
    monitor.spoutId = cmd.getOptionValue("spoutId");
    monitor.logstashHost = cmd.getOptionValue("logstashHost");
    monitor.logstashPort = Integer.parseInt(cmd.getOptionValue("logstashPort"));

    int refresh = Integer.parseInt(cmd.getOptionValue("refresh", "15"));

    Timer timer = new Timer();
    int period = refresh * 1000;
    int delay = 0;
    timer.schedule(monitor, delay, period);
  }

  String zkHosts;
  String zkRoot;
  String spoutId;
  String logstashHost;
  int logstashPort;

  final Logger logger = LoggerFactory.getLogger(KafakOffsetMonitor.class);

  @Override
  public void run() {
    try {
      action();
    } catch (IOException | KeeperException | InterruptedException e) {
      logger.error("Failed", e);
    }
  }

  public void action() throws IOException, KeeperException, InterruptedException {
    Watcher watcher = event -> logger.debug(event.toString());
    ZooKeeper zooKeeper = new ZooKeeper(zkHosts, 3000, watcher);

    Map<TopicAndPartition, Long> kafkaSpoutOffsets = new HashMap<>();
    Map<TopicAndPartition, SimpleConsumer> kafkaConsumers = new HashMap<>();

    List<String> children = zooKeeper.getChildren(zkRoot + "/" + spoutId, false);
    for (String child : children) {
      byte[] data = zooKeeper.getData(zkRoot + "/" + spoutId + "/" + child, false, null);
      String json = new String(data);
      Config kafkaSpoutOffset = ConfigFactory.parseString(json);

      String topic = kafkaSpoutOffset.getString("topic");
      int partition = kafkaSpoutOffset.getInt("partition");
      long offset = kafkaSpoutOffset.getLong("offset");
      String brokerHost = kafkaSpoutOffset.getString("broker.host");
      int brokerPort = kafkaSpoutOffset.getInt("broker.port");

      TopicAndPartition topicAndPartition = TopicAndPartition.apply(topic, partition);

      kafkaSpoutOffsets.put(topicAndPartition, offset);
      kafkaConsumers.put(topicAndPartition, new SimpleConsumer(brokerHost, brokerPort, 3000, 1024, "kafkaOffsetMonitor"));
    }
    zooKeeper.close();

    sendToLogstash(kafkaSpoutOffsets, kafkaConsumers);
  }

  private void sendToLogstash(Map<TopicAndPartition, Long> kafkaSpoutOffsets,
                              Map<TopicAndPartition, SimpleConsumer> kafkaConsumers) throws IOException {
    for (TopicAndPartition topicAndPartition : kafkaConsumers.keySet()) {
      Map<TopicAndPartition, PartitionOffsetRequestInfo> requestMap = new HashMap<>();
      requestMap.put(topicAndPartition, PartitionOffsetRequestInfo.apply(kafka.api.OffsetRequest.LatestTime(), 1));
      OffsetRequest offsetRequest = new OffsetRequest(requestMap, kafka.api.OffsetRequest.CurrentVersion(), kafka.api.OffsetRequest.DefaultClientId());

      SimpleConsumer consumer = kafkaConsumers.get(topicAndPartition);
      OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
      if (offsetResponse.hasError()) {
        System.out.println("error");
      } else {
        String topic = topicAndPartition.topic();
        int partition = topicAndPartition.partition();
        long head = offsetResponse.offsets(topic, partition)[0];
        long kafkaSpoutOffset = kafkaSpoutOffsets.get(topicAndPartition);

        XContentBuilder builder = jsonBuilder()
            .startObject()
            .field("topic", topic)
            .field("partition", partition)
            .field("kafkaSpoutOffset", kafkaSpoutOffset)
            .field("head", head)
            .field("gap", head - kafkaSpoutOffset)
            .field("application", "kafkaSpout")
            .endObject();
        String json = builder.string();

        try (Socket socket = new Socket(logstashHost, logstashPort)) {
          PrintWriter out =
              new PrintWriter(socket.getOutputStream(), true);
          out.println(json);
        }
      }
    }
  }

}
