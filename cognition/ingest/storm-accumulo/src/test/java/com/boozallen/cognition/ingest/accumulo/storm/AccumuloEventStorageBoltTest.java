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

package com.boozallen.cognition.ingest.accumulo.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import com.boozallen.cognition.ingest.accumulo.utils.AccumuloBoltUtils;
import com.boozallen.cognition.ingest.storm.PrepareFailedException;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import mockit.*;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.configuration.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

import static com.boozallen.cognition.ingest.accumulo.storm.AccumuloEventStorageBolt.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author bentse
 */
public class AccumuloEventStorageBoltTest {
  @Tested
  AccumuloEventStorageBolt bolt;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConfigureAccumuloBolt(@Injectable Configuration conf) {
    new Expectations() {{
      conf.getString(EVENT_TABLE);
      result = "eventTable";
      conf.getString(UUID_PREFIX, UUID_PREFIX_DEFAULT);
      result = UUID_PREFIX_DEFAULT;
      conf.getInt(SPLITS, SPLITS_DEFAULT);
      result = SPLITS_DEFAULT;
      conf.getString(VISIBILITY);
      result = "";
      conf.getString(VISIBILITY_BY_FIELD);
      result = "";
    }};

    bolt.configureAccumuloBolt(conf);
  }

  @Test
  public void testPrepareAccumuloBolt(
      @Injectable Map stormConf,
      @Injectable TopologyContext context) {
    new Expectations(bolt) {{
      bolt.resetEventWriter();
    }};

    bolt.prepareAccumuloBolt(stormConf, context);
  }

  @Test
  public void testResetEventWriter(
      @Injectable BatchWriter eventWriter,
      @Injectable Connector connector,
      @Injectable String tableName,
      @Injectable BatchWriterConfig batchWriterConfig) throws TableNotFoundException {

    bolt.eventWriter = null;
    bolt.conn = connector;
    bolt.eventTable = tableName;
    bolt.config = batchWriterConfig;

    new Expectations(bolt) {{
      connector.createBatchWriter(tableName, batchWriterConfig);
      result = eventWriter;
    }};

    bolt.resetEventWriter();
  }

  @Test
  public void testResetEventWriterWithCleanup(
      @Injectable BatchWriter eventWriter,
      @Injectable Connector connector,
      @Injectable String tableName,
      @Injectable BatchWriterConfig batchWriterConfig) throws TableNotFoundException {

    bolt.eventWriter = eventWriter;
    bolt.conn = connector;
    bolt.eventTable = tableName;
    bolt.config = batchWriterConfig;

    new Expectations(bolt) {{
      bolt.cleanupEventWriter();
      connector.createBatchWriter(tableName, batchWriterConfig);
      result = eventWriter;
    }};

    bolt.resetEventWriter();
  }

  @Test
  public void testResetEventWriterException(
      @Injectable BatchWriter eventWriter,
      @Injectable Connector connector,
      @Injectable String tableName,
      @Injectable BatchWriterConfig batchWriterConfig,
      @Injectable TableNotFoundException e) throws Exception {

    bolt.eventWriter = eventWriter;
    bolt.conn = connector;
    bolt.eventTable = tableName;
    bolt.config = batchWriterConfig;

    new Expectations(bolt) {{
      bolt.cleanupEventWriter();
      connector.createBatchWriter(tableName, batchWriterConfig);
      result = e;
    }};

    thrown.expect(PrepareFailedException.class);
    bolt.resetEventWriter();
  }

  @Test
  public void testCleanupEventWriter(@Injectable BatchWriter eventWriter) throws Exception {
    bolt.eventWriter = eventWriter;

    new Expectations() {{
      eventWriter.flush();
      eventWriter.close();
    }};

    bolt.cleanupEventWriter();
  }

  @Test
  public void testCleanupEventWriterWithoutCleanup() throws Exception {
    bolt.eventWriter = null;

    bolt.cleanupEventWriter();
  }

  @Test
  public void testCleanupEventWriterException(
      @Injectable BatchWriter eventWriter,
      @Injectable MutationsRejectedException e) throws Exception {
    bolt.eventWriter = eventWriter;

    new Expectations() {{
      eventWriter.flush();
      result = e;
    }};

    bolt.cleanupEventWriter();
  }

  @Test
  public void testProcess(
      @Injectable LogRecord record,
      @Injectable BatchWriter eventWriter,
      @Injectable Mutation event) throws Exception {
    bolt.eventWriter = eventWriter;

    new Expectations(bolt) {{
      bolt.getEventMutation(record);
      result = event;
      eventWriter.addMutation(event);
    }};

    bolt.process(record);
  }

  @Test
  public void testProcessException(
      @Injectable LogRecord record,
      @Injectable BatchWriter eventWriter,
      @Injectable Mutation event,
      @Injectable MutationsRejectedException e) throws Exception {
    bolt.eventWriter = eventWriter;

    new Expectations(bolt) {{
      bolt.getEventMutation(record);
      result = event;
      eventWriter.addMutation(event);
      result = e;
      bolt.resetEventWriter();
    }};

    thrown.expect(FailedException.class);
    bolt.process(record);
  }

  @Test
  public void testGetEventMutation(
      @Injectable LogRecord record,
      @Mocked Mutation event,
      @Injectable String uuidPrefix,
      @Injectable String recId,
      @Injectable ColumnVisibility vis,
      @Injectable Map<String, String> map) throws Exception {
    bolt.uuidPrefix = uuidPrefix;
    bolt.splits = AccumuloEventStorageBolt.SPLITS_DEFAULT;

    new MockUp<AccumuloBoltUtils>() {
      @Mock
      String getEventRecordId(LogRecord record, String prefix, int splits) {
        return recId;
      }
    };

    new Expectations(bolt) {{
      new Mutation(recId);
      result = event;

      bolt.getColumnVisibility(record);
      result = vis;

      record.getFields();
      result = map;
      bolt.populateMutation(event, "data", vis, map);
      record.getMetadata();
      result = map;
      bolt.populateMutation(event, "metadata", vis, map);
    }};

    assertThat(bolt.getEventMutation(record), is(event));
  }

  @Test
  public void testGetColumnVisibility(
      @Injectable LogRecord record,
      @Mocked ColumnVisibility vis) {
    bolt.visibility = "visibility";

    new Expectations() {{
      new ColumnVisibility(bolt.visibility);
      result = vis;
    }};

    assertThat(bolt.getColumnVisibility(record), is(vis));
  }

  @Test
  public void testGetColumnVisibilityByField(
      @Injectable LogRecord record,
      @Mocked ColumnVisibility vis,
      @Injectable String visibility) {
    bolt.visibility = null;
    bolt.visibilityByField = "visibility-field";

    new Expectations() {{
      record.getValue(bolt.visibilityByField);
      result = visibility;
      new ColumnVisibility(visibility);
      result = vis;
    }};

    assertThat(bolt.getColumnVisibility(record), is(vis));
  }

  @Test
  public void testGetColumnVisibilityBlank(
      @Injectable LogRecord record,
      @Mocked ColumnVisibility vis) {
    bolt.visibility = null;
    bolt.visibilityByField = null;

    new Expectations() {{
      new ColumnVisibility();
      result = vis;
    }};

    assertThat(bolt.getColumnVisibility(record), is(vis));
  }

  @Test
  public void testPopulateMutation(
      @Injectable Mutation mut,
      @Injectable String columnFamily,
      @Injectable ColumnVisibility vis) throws Exception {
    Map<String, String> fields = new HashMap<>();
    fields.put("key", "value");

    new Expectations() {{
      mut.put(columnFamily, "key", vis, (Value) any);
    }};
    bolt.populateMutation(mut, columnFamily, vis, fields);
  }

  @Test
  public void testCleanup() {
    new Expectations(bolt) {{
      bolt.cleanupEventWriter();
    }};
    bolt.cleanup();
  }
}