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

import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import org.apache.commons.configuration.Configuration;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author bentse
 */
public class SkipBlankBoltTest {

  @Tested
  SkipBlankBolt bolt;

  @Test
  public void testConfigure(@Injectable Configuration conf) {
    new Expectations() {{
      conf.getString(SkipBlankBolt.FIELD);
      result = "field0";
    }};
    bolt.configure(conf);
    assertThat(bolt.field, is("field0"));
  }

  @Test
  public void testShouldEmit0(@Mocked final LogRecord record) throws Exception {
    bolt.field = "field";

    new Expectations() {{
      record.getValue(bolt.field);
      result = "asdf";
    }};

    Assert.assertThat(bolt.shouldEmit(record), Is.is(true));
  }

  @Test
  public void testShouldEmit1(@Mocked final LogRecord record) throws Exception {
    bolt.field = "field";

    new Expectations() {{
      record.getValue(bolt.field);
      result = "";
    }};

    Assert.assertThat(bolt.shouldEmit(record), Is.is(false));
  }
}