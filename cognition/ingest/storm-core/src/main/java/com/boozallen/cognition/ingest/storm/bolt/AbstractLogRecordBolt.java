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

package com.boozallen.cognition.ingest.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.boozallen.cognition.ingest.storm.Configurable;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;

/**
 * Base class for most cognition bolts. Usually extended by first bolt after spout, which reads incoming tuple and
 * creates {@link LogRecord} for downstream processing.
 */
public abstract class AbstractLogRecordBolt extends BaseBasicBolt implements Configurable {

  /**
   *
   */
  public static final String RECORD = "record";

  /**
   *
   */
  public static final String SHA1_CHECKSUM = "sha1_checksum";

  @Override
  final public void execute(Tuple input, BasicOutputCollector collector) {
    execute(input, record -> {
      collector.emit(new Values(record));
    });
  }

  protected abstract void execute(Tuple input, RecordCollector collector);

  @Override
  public final void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(RECORD));
  }

  public interface RecordCollector {
    void emit(LogRecord record);
  }
}
