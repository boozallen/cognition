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

package com.boozallen.cognition.ingest.accumulo.utils;

import com.boozallen.cognition.ingest.storm.vo.LogRecord;

public class AccumuloBoltUtils {
  public static final int SHARD_RADIX = 36;

  /**
   * Creates date based id for accumulo row id. RowId design avoids monotonically increasing row-keys problem by
   * sharding entries from the date based on split.
   *
   * @param record
   * @param prefix
   * @param splits
   * @return
   */
  public static String getEventRecordId(LogRecord record, String prefix, int splits) {
    String uuid = record.getUUID();
    String shard = getShard(uuid.hashCode(), splits);
    long time = record.getDate().getTime();
    return String.format("%s%s_%s_%s", prefix, shard, time, uuid);
  }

  public static String getShard(int hashcode, int splits) {
    return Integer.toString(Math.abs(hashcode % splits), SHARD_RADIX);
  }
}
