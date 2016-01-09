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

import com.boozallen.cognition.test.utils.TestResourceUtils;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author bentse
 */
public class GeoJsonPointReverseArrayBoltTest {
  @Tested
  GeoJsonPointReverseArrayBolt bolt;

  @Test
  public void testConfigure() throws Exception {
    URL resource = TestResourceUtils.getResource(this.getClass(), "config.xml");
    XMLConfiguration conf = new XMLConfiguration(resource);
    bolt.configure(conf);

    assertThat(bolt.srcDestFieldMapping.size(), is(2));
  }

  @Test
  public void testProcess(@Injectable LogRecord record) throws Exception {
    Map<String, String> map = new HashMap<>();
    map.put("src", "dest");
    bolt.srcDestFieldMapping = map;

    new Expectations(bolt) {{
      record.getValue("src");
      result = "srcString";
      bolt.reverseJsonArray("srcString");
      result = "destString";
      record.setValue("dest", "destString");
    }};

    bolt.process(record);
  }

  @Test
  public void testReverseJsonArray() throws Exception {
    assertThat(bolt.reverseJsonArray(""), is(""));
    assertThat(bolt.reverseJsonArray(null), is(""));
    assertThat(bolt.reverseJsonArray("[1.01,2.02]"), is("[2.02,1.01]"));
  }
}