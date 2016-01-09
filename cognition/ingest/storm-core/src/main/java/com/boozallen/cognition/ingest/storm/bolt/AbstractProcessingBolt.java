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

import backtype.storm.tuple.Tuple;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;

/**
 * Base class that allows enrichment on given {@link LogRecord} from upstream bolt. Auto emit to downstream bolts.
 */
public abstract class AbstractProcessingBolt extends AbstractLogRecordBolt {

  @Override
  final protected void execute(Tuple input, RecordCollector collector) {
    LogRecord record = (LogRecord) input.getValueByField(RECORD);
    process(record);
    collector.emit(record);
  }

  /**
   * @param record
   */
  protected abstract void process(LogRecord record);
}
