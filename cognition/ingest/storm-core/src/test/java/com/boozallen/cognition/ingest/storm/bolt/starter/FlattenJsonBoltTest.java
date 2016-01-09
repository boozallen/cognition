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

import backtype.storm.tuple.Tuple;
import com.boozallen.cognition.ingest.storm.bolt.AbstractLogRecordBolt;
import com.boozallen.cognition.ingest.storm.bolt.starter.FlattenJsonBolt;
import com.boozallen.cognition.ingest.storm.util.IngestUtilities;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import com.boozallen.cognition.test.utils.TestResourceUtils;
import com.typesafe.config.*;
import mockit.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class FlattenJsonBoltTest {
  @Tested
  FlattenJsonBolt bolt;

  @Test
  public void testExecute(
      @Injectable Tuple input,
      @Injectable AbstractLogRecordBolt.RecordCollector collector,
      @Mocked LogRecord logRecord) {
    String tupleValue = "tuple-value";
    new Expectations(bolt) {{
      input.getValue(0);
      result = tupleValue.getBytes();
      new LogRecord(anyString);
      result = logRecord;
      bolt.parseJson(anyString, logRecord);
      collector.emit(logRecord);
    }};

    bolt.execute(input, collector);
  }

  @Test
  public void testExecuteBlank(
      @Injectable Tuple input,
      @Injectable AbstractLogRecordBolt.RecordCollector collector) {
    String tupleValue = "";
    new Expectations(bolt) {{
      input.getValue(0);
      result = tupleValue.getBytes();
      bolt.parseJson(anyString, (LogRecord) any);
      times = 0;
      collector.emit((LogRecord) any);
      times = 0;
    }};

    bolt.execute(input, collector);
  }

  @Test
  public void testExecuteException(
      @Injectable Tuple input,
      @Injectable AbstractLogRecordBolt.RecordCollector collector,
      @Mocked LogRecord logRecord,
      @Injectable Exception e) {
    String tupleValue = "tuple-value";
    new Expectations(bolt) {{
      input.getValue(0);
      result = tupleValue.getBytes();
      new LogRecord(anyString);
      result = logRecord;
      bolt.parseJson(anyString, logRecord);
      result = e;
      collector.emit((LogRecord) any);
      times = 0;
    }};

    bolt.execute(input, collector);
  }

  @Test
  public void testParseJson() throws IOException {
    try (InputStream sampleDataStream = TestResourceUtils.getResourceAsStream(this.getClass(), "test.json")) {
      List<String> stringList = IOUtils.readLines(sampleDataStream);
      String jsonString = StringUtils.join(stringList, "");

      LogRecord logRecord = new LogRecord();
      bolt.parseJson(jsonString, logRecord);

      Map<String, String> fields = logRecord.getFields();
      assertThat(fields.size(), is(6));
      assertThat(fields.get("boolean"), is("true"));
      assertThat(fields.get("object.a"), is("a"));
      assertThat(fields.get("number"), is("1234"));
      assertThat(fields.get("string"), is("string"));
      assertThat(fields.get("list"), is("[\"value0\",\"value1\"]"));
      assertThat(fields.get("object_list"), is("[{\"a\":\"a\"},{\"b\":\"b\"}]"));
    }
  }

  @Test
  public void testParseJsonDataswift() throws IOException {
    try (InputStream sampleDataStream = TestResourceUtils.getResourceAsStream(this.getClass(), "datasift-json-twitter.txt")) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(sampleDataStream));
      String jsonString = reader.readLine();

      LogRecord logRecord = new LogRecord();
      bolt.parseJson(jsonString, logRecord);

      Map<String, String> fields = logRecord.getFields();
      assertThat(fields.size(), is(61));
      assertThat(fields.get("language.confidence"), is("96"));
      assertThat(fields.get("interaction.mention_ids"), is("[501181374]"));
      assertThat(fields.get("interaction.author.username"), is("itsLindsay_2"));
    }
  }

  @Test
  public void testParseJsonGnip() throws IOException {
    try (InputStream sampleDataStream = TestResourceUtils.getResourceAsStream(this.getClass(), "gnip-json-twitter.txt")) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(sampleDataStream));
      String jsonString = reader.readLine();

      LogRecord logRecord = new LogRecord();
      bolt.parseJson(jsonString, logRecord);

      Map<String, String> fields = logRecord.getFields();
      assertThat(fields.size(), is(46));
      assertThat(fields.get("id"), is("tag:search.twitter.com,2005:620019979149266949"));
      assertThat(fields.get("actor.link"), is("http://www.twitter.com/legazacadmo"));
      assertThat(fields.get("actor.statusesCount"), is("104048"));
    }
  }

  @Test
  public void testParseJsonGnipWithUnprintableCharacter() throws IOException {
    try (InputStream sampleDataStream = TestResourceUtils.getResourceAsStream(this.getClass(), "gnip-json-twitter-unprintable.txt")) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(sampleDataStream));
      String jsonString = reader.readLine();

      LogRecord logRecord = new LogRecord();
      bolt.parseJson(jsonString, logRecord);

      Map<String, String> fields = logRecord.getFields();
      assertThat(fields.get("actor.summary"), is("着� ぐ る み と お 絵 描 き 好 き の ね こ で す 。"));
      assertThat(fields.get("object.summary"), is("@eve_flower_ 戦士Lvが43にあがった！(+8) しつこさ、なだかさ、あらっぽさ等があがった！(メダル+8) 【ログボGET→ https://t.co/sz92Er3XuQ 】 #lvup"));
    }
  }

  @Test
  public void testParseJsonBoolean(
      @Injectable Config config,
      @Injectable Map.Entry<String, ConfigValue> entry,
      @Injectable ConfigValue configValue,
      @Injectable String jsonString,
      @Injectable LogRecord logRecord) throws IOException {

    setupParseJsonMock(config, jsonString);

    Set<Map.Entry<String, ConfigValue>> entrySet = new HashSet<>();
    entrySet.add(entry);
    new Expectations() {{
      config.entrySet();
      result = entrySet;
      entry.getKey();
      result = "key";
      entry.getValue();
      result = configValue;
      configValue.valueType();
      result = ConfigValueType.BOOLEAN;

      configValue.unwrapped();
      result = Boolean.TRUE;
      logRecord.setValue("key", "true");
    }};
    bolt.parseJson(jsonString, logRecord);
  }

  @Test
  public void testParseJsonNumber(
      @Injectable Config config,
      @Injectable Map.Entry<String, ConfigValue> entry,
      @Injectable ConfigValue configValue,
      @Injectable String jsonString,
      @Injectable LogRecord logRecord) throws IOException {

    setupParseJsonMock(config, jsonString);

    Set<Map.Entry<String, ConfigValue>> entrySet = new HashSet<>();
    entrySet.add(entry);
    new Expectations() {{
      config.entrySet();
      result = entrySet;
      entry.getKey();
      result = "key";
      entry.getValue();
      result = configValue;
      configValue.valueType();
      result = ConfigValueType.NUMBER;

      configValue.unwrapped();
      result = new Long(1234);
      logRecord.setValue("key", "1234");
    }};
    bolt.parseJson(jsonString, logRecord);
  }

  @Test
  public void testParseJsonObject(
      @Injectable Config config,
      @Injectable Map.Entry<String, ConfigValue> entry,
      @Injectable ConfigValue configValue,
      @Injectable String jsonString,
      @Injectable LogRecord logRecord) throws IOException {

    setupParseJsonMock(config, jsonString);

    Set<Map.Entry<String, ConfigValue>> entrySet = new HashSet<>();
    entrySet.add(entry);
    Map<String, Object> object = new HashMap<>();
    object.put("a", "b");
    object.put("c", "d");
    new Expectations() {{
      config.entrySet();
      result = entrySet;
      entry.getKey();
      result = "key";
      entry.getValue();
      result = configValue;
      configValue.valueType();
      result = ConfigValueType.OBJECT;

      configValue.unwrapped();
      result = object;
      logRecord.setValue("key", "{a=b, c=d}");
    }};
    bolt.parseJson(jsonString, logRecord);
  }

  @Test
  public void testParseJsonString(
      @Injectable Config config,
      @Injectable Map.Entry<String, ConfigValue> entry,
      @Injectable ConfigValue configValue,
      @Injectable String jsonString,
      @Injectable LogRecord logRecord) throws IOException {

    setupParseJsonMock(config, jsonString);

    Set<Map.Entry<String, ConfigValue>> entrySet = new HashSet<>();
    entrySet.add(entry);
    new Expectations() {{
      config.entrySet();
      result = entrySet;
      entry.getKey();
      result = "key";
      entry.getValue();
      result = configValue;
      configValue.valueType();
      result = ConfigValueType.NUMBER;

      configValue.unwrapped();
      result = "string";
      logRecord.setValue("key", "string");
    }};
    bolt.parseJson(jsonString, logRecord);
  }

  @Test
  public void testParseJsonList(
      @Injectable Config config,
      @Injectable Map.Entry<String, ConfigValue> entry,
      @Injectable ConfigList configValue,
      @Injectable String jsonString,
      @Injectable LogRecord logRecord) throws IOException {

    setupParseJsonMock(config, jsonString);

    Set<Map.Entry<String, ConfigValue>> entrySet = new HashSet<>();
    entrySet.add(entry);
    List<Object> list = new ArrayList<>();
    list.add("a");
    list.add("b");
    new Expectations() {{
      config.entrySet();
      result = entrySet;
      entry.getKey();
      result = "key";
      entry.getValue();
      result = configValue;
      configValue.valueType();
      result = ConfigValueType.LIST;

      configValue.unwrapped();
      result = list;
      logRecord.setValue("key", "[\"a\",\"b\"]");
    }};
    bolt.parseJson(jsonString, logRecord);
  }

  @Test
  public void testParseJsonListNumber(
      @Injectable Config config,
      @Injectable Map.Entry<String, ConfigValue> entry,
      @Injectable ConfigList configValue,
      @Injectable String jsonString,
      @Injectable LogRecord logRecord) throws IOException {

    setupParseJsonMock(config, jsonString);

    Set<Map.Entry<String, ConfigValue>> entrySet = new HashSet<>();
    entrySet.add(entry);
    List<Object> list = new ArrayList<>();
    list.add(54.999444D);
    list.add(-1.544167D);
    new Expectations() {{
      config.entrySet();
      result = entrySet;
      entry.getKey();
      result = "key";
      entry.getValue();
      result = configValue;
      configValue.valueType();
      result = ConfigValueType.LIST;

      configValue.unwrapped();
      result = list;
      logRecord.setValue("key", "[54.999444,-1.544167]");
    }};
    bolt.parseJson(jsonString, logRecord);
  }

  @Test
  public void testParseJsonNull(
      @Injectable Config config,
      @Injectable Map.Entry<String, ConfigValue> entry,
      @Injectable ConfigList configValue,
      @Injectable String jsonString,
      @Injectable LogRecord logRecord) throws IOException {

    setupParseJsonMock(config, jsonString);

    Set<Map.Entry<String, ConfigValue>> entrySet = new HashSet<>();
    entrySet.add(entry);
    new Expectations() {{
      config.entrySet();
      result = entrySet;
      entry.getKey();
      result = "key";
      entry.getValue();
      result = configValue;
      configValue.valueType();
      result = ConfigValueType.NULL;
      logRecord.setValue(anyString, anyString);
      times = 0;

    }};
    bolt.parseJson(jsonString, logRecord);
  }

  private void setupParseJsonMock(@Injectable final Config config, @Injectable final String jsonString) {
    new MockUp<ConfigFactory>() {
      @Mock
      Config parseString(String s) {
        return config;
      }
    };
    new MockUp<IngestUtilities>() {
      @Mock
      String removeUnprintableCharacters(String string) {
        return jsonString;
      }
    };
  }
}
