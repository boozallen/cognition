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

import com.boozallen.cognition.ingest.storm.bolt.starter.LineRegexReplaceInRegionBolt;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import mockit.Injectable;
import mockit.StrictExpectations;
import mockit.Tested;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Test;

import java.util.Arrays;
import java.util.regex.Pattern;

import static com.boozallen.cognition.test.utils.TestResourceUtils.getResource;
import static com.boozallen.cognition.test.utils.TestResourceUtils.getResourceAsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * @author bentse
 */
public class LineRegexReplaceInRegionBoltTest {

  @Tested
  LineRegexReplaceInRegionBolt bolt;

  @Test
  public void testConfigure(@Injectable Configuration conf) throws Exception {
    new StrictExpectations(bolt) {{
      bolt.configureRegexRegions(conf);
      bolt.configureFields(conf);
      bolt.configureDelimiter(conf);
    }};

    bolt.configure(conf);
  }

  @Test
  public void testConfigureRegexRegions() throws Exception {
    XMLConfiguration conf = new XMLConfiguration(getResource(this.getClass(), "regexRegions.xml"));

    bolt.configureRegexRegions(conf);

    assertThat(bolt.groupSearchReplaceList.size(), is(2));

    Triple<Pattern, String, String> entry0 = bolt.groupSearchReplaceList.get(0);
    Triple<Pattern, String, String> entry1 = bolt.groupSearchReplaceList.get(1);

    assertNotNull(entry0.getLeft());
    assertThat(entry0.getMiddle(), is("regex0"));
    assertThat(entry0.getRight(), is("replacement0"));
    assertNotNull(entry1.getLeft());
    assertThat(entry1.getMiddle(), is("regex1"));
    assertThat(entry1.getRight(), is("replacement1"));
  }

  @Test
  public void testConfigureFields() throws Exception {
    XMLConfiguration conf = new XMLConfiguration(getResource(this.getClass(), "fields.xml"));

    bolt.configureFields(conf);

    assertThat(bolt.fieldList.size(), is(3));
  }

  @Test
  public void testConfigureDelimiter() throws Exception {
    XMLConfiguration conf = new XMLConfiguration(getResource(this.getClass(), "delimiter.xml"));

    bolt.configureDelimiter(conf);

    assertThat(bolt.delimiter, is('\t'));
  }

  @Test
  public void testReplace() throws Exception {
    String record = getResourceAsString(this.getClass(), "testReplace.txt");
    Pattern pattern = Pattern.compile("^([^\"]+)");
    String regex = "[ ]";
    String replacement = "|";
    String result = bolt.replace(record, pattern, regex, replacement);

    assertThat(result, is(getResourceAsString(this.getClass(), "testReplaceResult.txt")));
  }

  @Test
  public void testPopulateLogRecord() throws Exception {
    String record = getResourceAsString(this.getClass(), "testPopulateLogRecord.txt");

    bolt.delimiter = '\t';
    bolt.fieldList = Arrays.asList(
        "date",
        "time",
        "src_user",
        "src_ip",
        "cs_method",
        "cs_uri_stem",
        "protocol",
        "r_ip",
        "http_response",
        "rs_bytes",
        "time_taken",
        "s_ip",
        "s_action",
        "bytes_out",
        "bytes_in"
    );
    LogRecord logRecord = new LogRecord();

    bolt.populateLogRecord(logRecord, record);

    assertThat(logRecord.getValue("date"), is("2015-01-02"));
    assertThat(logRecord.getValue("r_ip"), is("1.2.3.4"));
  }
}