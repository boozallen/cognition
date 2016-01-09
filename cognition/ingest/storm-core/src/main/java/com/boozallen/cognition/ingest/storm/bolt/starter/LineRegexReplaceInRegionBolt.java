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

import au.com.bytecode.opencsv.CSVReader;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Tuple;
import com.boozallen.cognition.ingest.storm.bolt.AbstractLogRecordBolt;
import com.boozallen.cognition.ingest.storm.ConfigurationException;
import com.boozallen.cognition.ingest.storm.util.IngestUtilities;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author bentse
 */
public class LineRegexReplaceInRegionBolt extends AbstractLogRecordBolt {

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private static final String REGEX_REGION_GROUP = "regexRegions.region.group";
  private static final String REGEX_REGION_SEARCH = "regexRegions.region.search";
  private static final String REGEX_REGION_REPLACE = "regexRegions.region.replace";
  private static final String FIELDS = "fields.field";
  private static final String DELIMITER = "delimiter";

  //  <group-pattern, search-regex, replacement>
  List<Triple<Pattern, String, String>> groupSearchReplaceList;
  List<String> fieldList;
  char delimiter;

  @Override
  public void configure(Configuration conf) throws ConfigurationException {
    configureRegexRegions(conf);
    configureFields(conf);
    configureDelimiter(conf);
  }

  void configureRegexRegions(Configuration conf) throws ConfigurationException {
    List<String> regexRegionGroupList = conf.getList(REGEX_REGION_GROUP).stream().map(o -> o.toString()).collect(Collectors.toList());
    List<String> regexRegionSearchList = conf.getList(REGEX_REGION_SEARCH).stream().map(o -> o.toString()).collect(Collectors.toList());
    List<String> regexRegionReplaceList = conf.getList(REGEX_REGION_REPLACE).stream().map(o -> o.toString()).collect(Collectors.toList());

    int groupListSize = regexRegionGroupList.size();
    int searchListSize = regexRegionSearchList.size();
    int replaceListSize = regexRegionReplaceList.size();
    if (!(groupListSize == searchListSize && searchListSize == replaceListSize)) {
      // all lists must be the same size
      throw new ConfigurationException("Error initializing class. All regexRegion lists must be the same size");
    }

    groupSearchReplaceList = new ArrayList<>(groupListSize);
    for (int index = 0; index < regexRegionGroupList.size(); index++) {
      Pattern pattern = Pattern.compile(regexRegionGroupList.get(index));
      String regex = regexRegionSearchList.get(index);
      String replacement = regexRegionReplaceList.get(index);

      ImmutableTriple<Pattern, String, String> triple = new ImmutableTriple<>(pattern, regex, replacement);
      groupSearchReplaceList.add(triple);
    }
  }

  void configureFields(Configuration conf) {
    fieldList = conf.getList(FIELDS).stream().map(o -> o.toString()).collect(Collectors.toList());
	}

  void configureDelimiter(Configuration conf) {
    delimiter = IngestUtilities.getDelimiterByName(conf.getString(DELIMITER, "SPACE"));
  }

  @Override
  protected void execute(Tuple tuple, RecordCollector collector) {
    byte[] bytes = (byte[]) tuple.getValue(0);
    String record = new String(bytes);
    String sha1Checksum = DigestUtils.shaHex(bytes);

    if (StringUtils.isBlank(record)) {
      // skips blank entries
      logger.info("received blank record");
      return;
    }

    LogRecord logRecord = new LogRecord(sha1Checksum);
    logRecord.addMetadataValue(SHA1_CHECKSUM, sha1Checksum);

    String recordAfterReplace = replaceAll(record);
    populateLogRecord(logRecord, recordAfterReplace);

    collector.emit(logRecord);
  }

  String replaceAll(String record) {
    for (Triple<Pattern, String, String> entry : groupSearchReplaceList) {
      Pattern pattern = entry.getLeft();
      String regex = entry.getMiddle();
      String replacement = entry.getRight();

      record = replace(record, pattern, regex, replacement);
    }
    return record;
  }

  String replace(String record, Pattern pattern, String regex, String replacement) {
    Matcher match = pattern.matcher(record);
    if (match.find() && match.groupCount() > 0) {
      // only replace the first group
      int startPos = match.start(1);
      int stopPos = match.end(1);

      String replaceString = match.group(1).replaceAll(regex, replacement);
      return record.substring(0, startPos) + replaceString + record.substring(stopPos);
    } else {
      // no match, returns original
      return record;
    }
  }

  void populateLogRecord(LogRecord logRecord, String record) {	  
    try (CSVReader csvReader = new CSVReader(new StringReader(record), delimiter)) {
      String[] values = csvReader.readNext();
      
      int fieldSize = Math.min(fieldList.size(), values.length);
      for (int i = 0; i < fieldSize; i++) {
        String field = fieldList.get(i);
        String value = values[i];
        logRecord.setValue(field, value);
}
      
    } catch (IOException e) {
      logger.error("Failed to parse line: {}", record);
      throw new FailedException(e);
    }
  }

}
