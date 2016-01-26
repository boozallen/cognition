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

import static org.junit.Assert.*;

import java.time.Instant;
import java.util.Collection;

import org.apache.accumulo.core.data.Range;
import org.junit.Test;


public class AccumuloUtilsTester {

  @Test
  public void testGetRanges() {
    Collection<Range> ranges = AccumuloUtils.getRanges(Instant.parse("2014-12-08T00:00:00.00Z"),
        Instant.parse("2014-12-08T00:00:00.00Z"),
        "TWITTER");

    // Check the size
    assertEquals(36, ranges.size()); // 0-9, a-z

    for (Range range : ranges) {
      // Expecting StartKey: TWITTER_?_1417996800000 : [] 9223372036854775807 false
      assertTrue(range.getStartKey().toString().startsWith("TWITTER_"));
      assertTrue(range.getStartKey().toString().endsWith("_1417996800000 : [] 9223372036854775807 false"));
      assertTrue(range.isStartKeyInclusive());

      // Expecting EndKey: TWITTER_?_1417996801000 : [] 9223372036854775807 false
      assertTrue(range.getEndKey().toString().startsWith("TWITTER_"));
      assertTrue(range.getEndKey().toString().endsWith("_1417996801000 : [] 9223372036854775807 false"));
      assertFalse(range.isEndKeyInclusive());
    }
  }
}
