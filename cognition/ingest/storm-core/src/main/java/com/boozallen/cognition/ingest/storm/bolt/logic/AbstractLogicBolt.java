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

package com.boozallen.cognition.ingest.storm.bolt.logic;

import backtype.storm.tuple.Tuple;
import com.boozallen.cognition.ingest.storm.bolt.AbstractLogRecordBolt;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;

/**
 * Base class for logic bolts. Receives {@link LogRecord} from upstream bolt, decide whether to emit downstream based on
 * logic.
 */
public abstract class AbstractLogicBolt extends AbstractLogRecordBolt {

  @Override
  final protected void execute(Tuple input, RecordCollector collector) {
    LogRecord record = (LogRecord) input.getValueByField(RECORD);
    if (shouldEmit(record))
      collector.emit(record);
  }

  /**
   * Decides if given record should be emitted down stream
   *
   * @param record
   * @return <code>true</code> if record should be emitted down stream, else <code>false</code>
   */
  protected abstract boolean shouldEmit(LogRecord record);
}
