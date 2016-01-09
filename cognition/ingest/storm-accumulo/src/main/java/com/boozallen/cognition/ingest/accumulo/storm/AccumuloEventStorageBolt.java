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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.commons.lang3.StringUtils.defaultString;

/**
 * Stores log records into the accumulo event table, using a date-based row-id.
 */
public class AccumuloEventStorageBolt extends AccumuloBaseBolt {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());
  public static final String EVENT_TABLE = "eventTable";
  public static final String UUID_PREFIX = "uuidPrefix";
  public static final String UUID_PREFIX_DEFAULT = "";
  public static final String VISIBILITY = "visibility";
  public static final String VISIBILITY_BY_FIELD = "visibilityByField";
  public static final String SPLITS = "splits";
  public static final int SPLITS_DEFAULT = 36;

  String eventTable;
  BatchWriter eventWriter;
  String uuidPrefix;
  String visibility;
  String visibilityByField;
  int splits;

  @Override
  void configureAccumuloBolt(Configuration conf) {
    eventTable = conf.getString(EVENT_TABLE);
    uuidPrefix = conf.getString(UUID_PREFIX, UUID_PREFIX_DEFAULT);
    splits = conf.getInt(SPLITS, SPLITS_DEFAULT);
    visibility = conf.getString(VISIBILITY);
    visibilityByField = conf.getString(VISIBILITY_BY_FIELD);

    Validate.notBlank(eventTable);
  }

  @Override
  void prepareAccumuloBolt(Map stormConf, TopologyContext context) {
    resetEventWriter();
  }

  void resetEventWriter() {
    try {
      if (eventWriter != null) {
        cleanupEventWriter();
      }
      eventWriter = conn.createBatchWriter(eventTable, config);
    } catch (TableNotFoundException e) {
      logger.error("Table not found", e);
      throw new PrepareFailedException("Table not found", e);
    }
  }

  void cleanupEventWriter() {
    try {
      if (eventWriter != null) {
        eventWriter.flush();
        eventWriter.close();
      }
    } catch (MutationsRejectedException e) {
      logger.error("Failed committing mutation batch mutation", e);
    }
  }

  @Override
  protected void process(LogRecord record) {
    Mutation event = getEventMutation(record);
    try {
      eventWriter.addMutation(event);
    } catch (MutationsRejectedException e) {
      logger.error("Failed to store row. Reseting Writer", e);
      resetEventWriter();
      throw new FailedException(e);
    }
  }

  Mutation getEventMutation(LogRecord record) {
    String recId = AccumuloBoltUtils.getEventRecordId(record, uuidPrefix, splits);
    Mutation mutation = new Mutation(recId);

    ColumnVisibility vis = getColumnVisibility(record);

    populateMutation(mutation, "data", vis, record.getFields());
    populateMutation(mutation, "metadata", vis, record.getMetadata());
    return mutation;
  }

  ColumnVisibility getColumnVisibility(LogRecord record) {
    if (StringUtils.isNotBlank(visibility)) {
      return new ColumnVisibility(visibility);
    } else if (StringUtils.isNotBlank(visibilityByField)) {
      return new ColumnVisibility(record.getValue(visibilityByField));
    } else {
      return new ColumnVisibility();
    }
  }

  void populateMutation(Mutation mut, String columnFamily, ColumnVisibility vis, Map<String, String> fields) {
    for (Map.Entry<String, String> entry : fields.entrySet()) {
      String key = defaultString(entry.getKey());
      String value = defaultString(entry.getValue());

      mut.put(columnFamily, key, vis, new Value(value.getBytes()));
    }
  }

  @Override
  public void cleanup() {
    cleanupEventWriter();
  }
}
