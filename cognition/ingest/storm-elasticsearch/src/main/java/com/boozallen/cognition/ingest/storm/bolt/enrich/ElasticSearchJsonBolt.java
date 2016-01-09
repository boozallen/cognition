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

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.boozallen.cognition.ingest.storm.Configurable;
import com.boozallen.cognition.ingest.storm.bolt.AbstractLogRecordBolt;
import com.boozallen.cognition.ingest.storm.util.ElasticsearchUtil;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import com.google.gson.Gson;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.boozallen.cognition.ingest.storm.util.ConfigurationMapEntryUtils.extractMapList;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Creates JSON for elasticsearch indexing from {@link LogRecord}.
 * <p>
 * <pre>
 * <table>
 *   <tr>
 *     <td>subscribeToBolt</td>
 *     <td>should be set to ElasticSearchJsonBolt bolt number</td>
 *   </tr>
 *   <tr>
 *     <td>streamId</td>
 *   <td>set to <code>es_json</code>, which is secondary stream from ElasticSearchJsonBolt containing JSON for
 * indexing</td>
 *   </tr>
 *   <tr>
 *     <td>manualFlushFreqSecs</td>
 *     <td>force flush frequency using tick tuple (builtin EsBolt function)</td>
 *   </tr>
 *   <tr>
 *     <td>es.*</td>
 *     <td>configuration passed to EsBolt</td>
 *   </tr>
 * </table>
 * </pre>
 * <p>
 * Example configuration:
 * <pre>
 * {@code
 *
 * <conf>
 *   <subscribeToBolt>9</subscribeToBolt>
 *   <streamId>es_json</streamId>
 *   <manualFlushFreqSecs>5</manualFlushFreqSecs>
 *   <es.resource.write>{index}/{cognition_dataType}</es.resource.write>
 *   <es.storm.bolt.write.ack>true</es.storm.bolt.write.ack>
 *   <es.batch.size.bytes>100mb</es.batch.size.bytes>
 *   <es.mapping.id>sha1_checksum</es.mapping.id>
 *   <es.mapping.timestamp>postedTime</es.mapping.timestamp>
 *   <es.input.json>true</es.input.json>
 *   <es.nodes>ELASTICSEARCH-NODE-0,ELASTICSEARCH-NODE-1,ELASTICSEARCH-NODE-2</es.nodes>
 * </conf>
 * }</pre>
 *
 * @author michaelkorb
 * @author bentse
 */
public class ElasticSearchJsonBolt extends BaseBasicBolt implements Configurable {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public static final String ES_JSON = "es_json";

  static final String INDEX_NAME = "indexName";
  static final String INDEX_NAME_DEFAULT = "cognition";
  static final String INDEX_FIELD = "indexField";
  static final String INDEX_FIELD_DEFAULT = "index";
  static final String ES_JSON_FIELD = "esJsonField";
  static final String ES_JSON_FIELD_DEFAULT = "esjson";

  static final String FIELD_NAME = "fieldName";
  static final String FIELD_TYPE = "fieldType";
  static final String DATE_FORMAT = "dateFormat";
  static final String INPUT_DATE_FORMAT = "inputDateFormat";
  static final String POSTFIX_DATE_FORMAT = "postfixDateFormat";

  String esJsonField;
  String indexField;
  String indexName;
  String timeSeriesIndexFieldName;
  String timeSeriesIndexInputDateFormat;
  String timeSeriesIndexOutputDateFormat;
  IndexNameBuilder indexNameBuilder;

  Map<String, Map<String, String>> fieldTypeMappings;

  @Override
  public void configure(Configuration conf) {
    configureBolt(conf);
    configureTimeSeriesIndex(conf.subset("timeSeriesIndex"));
    configureFieldTypeMapping(conf.subset("fieldTypeMapping"));
  }

  void configureBolt(Configuration conf) {
    esJsonField = conf.getString(ES_JSON_FIELD, ES_JSON_FIELD_DEFAULT);
    indexName = conf.getString(INDEX_NAME, INDEX_NAME_DEFAULT);
    indexField = conf.getString(INDEX_FIELD, INDEX_FIELD_DEFAULT);
  }

  void configureTimeSeriesIndex(Configuration timeSeriesIndexNameConfig) {
    if (!timeSeriesIndexNameConfig.isEmpty()) {
      timeSeriesIndexFieldName = timeSeriesIndexNameConfig.getString(FIELD_NAME);
      timeSeriesIndexInputDateFormat = timeSeriesIndexNameConfig.getString(INPUT_DATE_FORMAT);
      timeSeriesIndexOutputDateFormat = timeSeriesIndexNameConfig.getString(POSTFIX_DATE_FORMAT);
    }
  }

  void configureFieldTypeMapping(Configuration conf) {
    fieldTypeMappings = extractMapList(conf, "entry", FIELD_NAME, FIELD_TYPE, DATE_FORMAT);
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    if (StringUtils.isBlank(timeSeriesIndexFieldName)) {
      indexNameBuilder = new BaseIndexNameBuilder(indexName);
    } else {
      indexNameBuilder = new TimeSeriesIndexNameBuilder(
          indexName,
          timeSeriesIndexFieldName,
          timeSeriesIndexInputDateFormat,
          timeSeriesIndexOutputDateFormat);
    }
  }

  @Override
  public final void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(AbstractLogRecordBolt.RECORD));
    declarer.declareStream(ES_JSON, new Fields(ES_JSON));
  }

  @Override
  final public void execute(Tuple input, BasicOutputCollector collector) {
    LogRecord record = (LogRecord) input.getValueByField(AbstractLogRecordBolt.RECORD);

    try {
      indexRecord(record);
    } catch (Exception e) {
      logger.error("Error indexing record", e);
      throw new FailedException("Error indexing record", e);
    }
    collector.emit(new Values(record));
    collector.emit(ES_JSON, new Values(record.getValue(esJsonField)));
  }

  void indexRecord(LogRecord record) throws Exception {

    String indexName = indexNameBuilder.build(record.getFields());

    XContentBuilder source = jsonBuilder().startObject();
    for (Entry<String, String> entry : record.getMetadata().entrySet()) {
      addField(source, entry, fieldTypeMappings);
    }
    for (Entry<String, String> entry : record.getFields().entrySet()) {
      addField(source, entry, fieldTypeMappings);
    }

    source.field(indexField, indexName);

    source.endObject();
    if (StringUtils.isNotBlank(esJsonField)) {
      record.setValue(esJsonField, source.string());
    }
  }

  void addField(XContentBuilder source,
                Entry<String, String> entry,
                Map<String, Map<String, String>> fieldTypeMappings) throws IOException {
    String key = entry.getKey();
    String value = entry.getValue();

    if (StringUtils.isBlank(value)) {
      logger.debug("Skipping blank value for key: {}", key);
      return;
    }

    String cleanedKey = ElasticsearchUtil.cleanKey(key);

    Map<String, String> fieldTypeMapping = getFieldTypeMapping(fieldTypeMappings, cleanedKey);

    try {
      addFieldByType(source, cleanedKey, value, fieldTypeMapping);
    } catch (NumberFormatException | ParseException e) {
      logger.error("Failed to parse entry - {}:{}", key, value);
      throw new FailedException(e);
    }
  }

  Map<String, String> getFieldTypeMapping(Map<String, Map<String, String>> fieldTypeMappings, String cleanedKey) {
    boolean hasFieldMapping = fieldTypeMappings.containsKey(cleanedKey);
    Map<String, String> fieldTypeMapping = null;
    if (hasFieldMapping) {
      fieldTypeMapping = fieldTypeMappings.get(cleanedKey);
    } else {
      fieldTypeMapping = Collections.emptyMap();
    }
    return fieldTypeMapping;
  }

  void addFieldByType(XContentBuilder source,
                      String key,
                      String value,
                      Map<String, String> fieldTypeMapping) throws IOException, ParseException {

    String fieldType = fieldTypeMapping.get(FIELD_TYPE);
    if (equalsIgnoreCase(fieldType, "array")) {
      Gson gson = new Gson();
      List list = gson.fromJson(value, List.class);
      source.field(key, list);
    } else if (equalsIgnoreCase(fieldType, "date")) {
      String dateFormat = fieldTypeMapping.get(DATE_FORMAT);
      SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
      source.field(key, sdf.parse(value));
    } else if (equalsIgnoreCase(fieldType, "microseconds")) { // Unix timestamp with microseconds
      double microseconds = Double.parseDouble(value);
      double milliseconds = microseconds * 1000;
      source.field(key, new Date((long) milliseconds));
    } else {
      source.field(key, value);
    }
  }

  interface IndexNameBuilder {
    String build(Map<String, String> fields) throws Exception;
  }

  class BaseIndexNameBuilder implements IndexNameBuilder {
    private final String indexName;

    public BaseIndexNameBuilder(String indexName) {
      this.indexName = indexName;
    }

    @Override
    public String build(Map<String, String> fields) throws Exception {
      return indexName;
    }
  }

  class TimeSeriesIndexNameBuilder implements IndexNameBuilder {
    private final String fieldName;
    private final DateTimeFormatter inputDateFormatter;
    private final DateTimeFormatter outputDateFormatter;
    private final String indexName;

    public TimeSeriesIndexNameBuilder(String indexName, String timeSeriesIndexFieldName,
                                      String timeSeriesIndexInputDateFormat, String postfixDateFormat) {
      this.indexName = indexName;
      this.fieldName = timeSeriesIndexFieldName;

      inputDateFormatter = DateTimeFormatter.ofPattern(timeSeriesIndexInputDateFormat);
      outputDateFormatter = DateTimeFormatter.ofPattern(postfixDateFormat).withZone(ZoneId.of("UTC"));
    }

    @Override
    public String build(Map<String, String> fields) throws ParseException {
      String value = fields.get(fieldName);
      if (StringUtils.isBlank(value)) {
        logger.error("Blank date field for time series index name: {}", fieldName);
        throw new FailedException("Blank date field for time series index name " + fieldName);
      } else {
        TemporalAccessor date = inputDateFormatter.parse(value);
        return String.format("%s%s", indexName, outputDateFormatter.format(date));
      }
    }
  }
}
