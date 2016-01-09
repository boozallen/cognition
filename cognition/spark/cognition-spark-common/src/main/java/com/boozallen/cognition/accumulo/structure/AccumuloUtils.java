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

package com.boozallen.cognition.accumulo.structure;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.accumulo.core.data.Range;

public class AccumuloUtils {

  /**
   * Constructs the accumulo ranges with the correct times, shards and sources.
   * @param beginTime -- the Instant to start the range
   * @param endTime -- the Instant to end the range
   * @param source -- the source of the information
   * @return
   */
  public static Collection<Range> getRanges(Instant beginTime, Instant endTime, Source source) {
    ArrayList<Range> ranges = new ArrayList<>();
    //numeric shard ids
    long begin = beginTime.toEpochMilli();
    long end = endTime.toEpochMilli();
    end += 1000; //needed so it can be exclusive, otherwise range is not correct
    for (int i = 0; i <= 9; i++) {
      Range range = new Range(source.toString() + "_" + i + "_" + begin, true, source.toString() + "_" + i + "_" + end, false);
      ranges.add(range);
    }

    for (char i = 'a'; i <= 'z'; i++) {
      Range range = new Range(source.toString() + "_" + i + "_" + begin, true, source.toString() + "_" + i + "_" + end, false);
      ranges.add(range);
    }
    return ranges;
  }
}
