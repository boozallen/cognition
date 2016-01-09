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

package com.boozallen.cognition.ingest.storm.bolt.starter;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Tuple;
import com.boozallen.cognition.ingest.storm.bolt.AbstractLogRecordBolt;
import com.boozallen.cognition.ingest.storm.bolt.csv.CsvLogRecordParser;
import com.boozallen.cognition.ingest.storm.bolt.csv.CsvLogRecordParserConfig;
import com.boozallen.cognition.ingest.storm.ConfigurationException;
import com.boozallen.cognition.ingest.storm.vo.FileMetadata;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * Head of bolt chain. Receives CSV HDFS info from spout, fetch file and creates {@link LogRecord} from each CSV entries
 * for downstream processing.
 */
public class CsvHdfsBolt extends AbstractLogRecordBolt {
  public static final String HADOOP_CONF_DIRECTORY = "hadoopConfDirectory";
  public static final String HADOOP_CONFIG = "hadoopConfig";

  private static final Logger LOGGER = LoggerFactory.getLogger(CsvHdfsBolt.class);

  private CsvLogRecordParserConfig csvParserConfig;

  Map<String, String> _hadoopConfig = new HashMap<>();
  String hadoopConfDirectory;

  FileSystem fileSystem;


  @Override
  public void configure(Configuration conf) throws ConfigurationException {
    csvParserConfig = new CsvLogRecordParserConfig(conf);
    configureHadoop(conf);
  }

  void configureHadoop(Configuration conf) throws ConfigurationException {
    hadoopConfDirectory = conf.getString(HADOOP_CONF_DIRECTORY);
    Configuration hadoopConfigSubset = conf.subset(HADOOP_CONFIG);
    for (Iterator itr = hadoopConfigSubset.getKeys(); itr.hasNext(); ) {
      String key = (String) itr.next();
      String value = hadoopConfigSubset.getString(key);
      _hadoopConfig.put(key, value);
    }
    if (isBlank(hadoopConfDirectory) && _hadoopConfig.isEmpty()) {
      throw new ConfigurationException(
          String.format("Missing Hadoop configuration. Configure with either %s or %s.",
              HADOOP_CONFIG, HADOOP_CONF_DIRECTORY));
    }
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    try {
      prepareHDFS();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void prepareHDFS() throws IOException {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    if (_hadoopConfig.isEmpty()) {
      conf.addResource(new Path(hadoopConfDirectory + File.separator + "core-site.xml"));
      conf.addResource(new Path(hadoopConfDirectory + File.separator + "hdfs-site.xml"));
    } else {
      for (Map.Entry<String, String> entry : _hadoopConfig.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }

    fileSystem = FileSystem.get(conf);
  }

  @Override
  protected void execute(Tuple tuple, RecordCollector collector) {
    String metadataJson = new String((byte[]) tuple.getValue(0));

    try {
      FileMetadata fileMetadata = FileMetadata.parseJson(metadataJson);

      String filename = fileMetadata.getFilename();
      String hdfsPath = fileMetadata.getHdfsPath();
      String fileType = fileMetadata.getFileType();

      if (isBlank(filename) || isBlank(hdfsPath) || isBlank(fileType)) {
        LOGGER.error("Incomplete file metadata. Requires: filename, hdfsPath and fileType. {}", fileMetadata);
        throw new FailedException("Incomplete file metadata: " + fileMetadata);
      }

      try (FSDataInputStream fsDataInputStream = fileSystem.open(new Path(hdfsPath));
           InputStreamReader fileReader = new InputStreamReader(fsDataInputStream);) {

        CsvLogRecordParser parser = new CsvLogRecordParser(csvParserConfig);
        parser.parse(fileReader, fileType, logRecord -> {
          logRecord.setValue("filename", filename);
          logRecord.setValue("fileType", fileType);
          collector.emit(logRecord);
        });
      }
    } catch (IOException e) {
      LOGGER.error(metadataJson, e);
      throw new FailedException(e);
    }
  }
}
