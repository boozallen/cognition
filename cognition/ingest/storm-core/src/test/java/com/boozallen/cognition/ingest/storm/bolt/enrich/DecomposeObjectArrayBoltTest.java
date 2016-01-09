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

package com.boozallen.cognition.ingest.storm.bolt.enrich;

import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author bentse
 */
public class DecomposeObjectArrayBoltTest {

  @Tested
  DecomposeObjectArrayBolt bolt;

  @Test
  public void testConfigure(@Injectable Configuration conf) {
    new Expectations() {{
      conf.getString(DecomposeObjectArrayBolt.ARRAY_FIELD);
      result = "field0";
      conf.getString(DecomposeObjectArrayBolt.OBJECT_FIELD);
      result = "field1";
      conf.getString(DecomposeObjectArrayBolt.DEST_FIELD);
      result = "field2";
    }};
    bolt.configure(conf);
    assertThat(bolt.arrayField, is("field0"));
    assertThat(bolt.objectField, is("field1"));
    assertThat(bolt.destField, is("field2"));
  }

  @Test
  public void testProcess(@Injectable LogRecord record) throws Exception {
    bolt.arrayField = "array-field";
    bolt.objectField = "text";
    bolt.destField = "dest-field";

    new Expectations() {{
      record.getValue(bolt.arrayField);
      result = "[{\"indices\":[10,23],\"text\":\"CecilTheLion\"},{\"indices\":[72,85],\"text\":\"RinglingBros\"},{\"indices\":[112,131],\"text\":\"JusticeForAllLions\"}]";
      record.setValue(bolt.destField, "[\"CecilTheLion\",\"RinglingBros\",\"JusticeForAllLions\"]");

    }};
    bolt.process(record);
  }
}