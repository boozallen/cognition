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

import backtype.storm.tuple.Tuple;
import com.boozallen.cognition.ingest.storm.bolt.AbstractLogRecordBolt;
import com.boozallen.cognition.ingest.storm.util.IngestUtilities;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Parse and transform DataSift json to string key-value pairs and store into {@link LogRecord} for emitting
 * downstream.
 *
 * @author bentse
 */
public class FlattenJsonBolt extends AbstractLogRecordBolt {
  final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Override
  public void configure(Configuration conf) {
  }

  @Override
  protected void execute(Tuple input, RecordCollector collector) {
    byte[] bytes = (byte[]) input.getValue(0);
    String record = new String(bytes);
    String sha1Checksum = DigestUtils.shaHex(bytes);

    if (StringUtils.isBlank(record)) {
      // skips blank entries
      logger.info("received blank record");
      return;
    }
    try {
      LogRecord logRecord = new LogRecord(sha1Checksum);
      logRecord.addMetadataValue(SHA1_CHECKSUM, sha1Checksum);
      parseJson(record, logRecord);
      collector.emit(logRecord);
    } catch (Exception e) {
      // Not bubbling up, since it would fail the entire tuple
      // Parsing failure would not be fixed even after a replay...
      logger.error("Failed to process tuple: " + record, e);
    }
  }

  void parseJson(String jsonString, LogRecord logRecord) {
    String cleanedJsonString = IngestUtilities.removeUnprintableCharacters(jsonString);
    Config cfg = ConfigFactory.parseString(cleanedJsonString);
    for (Map.Entry<String, ConfigValue> entry : cfg.entrySet()) {
      String key = entry.getKey();
      ConfigValue value = entry.getValue();
      switch (value.valueType()) {
        case BOOLEAN:
        case NUMBER:
        case OBJECT:
        case STRING:
          logRecord.setValue(key, ObjectUtils.toString(value.unwrapped()));
          break;
        case LIST:
          ConfigList list = (ConfigList) value;
          Gson gson = new Gson();
          String json = gson.toJson(list.unwrapped());
          logRecord.setValue(key, json);
          break;
        case NULL:
        default:
          // skip
      }
    }
  }
}