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

package com.boozallen.cognition.ingest.storm.bolt.csv;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import com.boozallen.cognition.ingest.storm.util.ElasticsearchUtil;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;

/**
 * Creates {@link LogRecord} from CSV entries
 */
public class CsvLogRecordParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvLogRecordParser.class);
  private final CsvLogRecordParserConfig config;

  public CsvLogRecordParser(CsvLogRecordParserConfig csvLogRecordParserConfig) {
    this.config = csvLogRecordParserConfig;
  }

  public void parse(Reader fileReader, String fileType, LogRecordCollector collector) throws IOException {
    try (CSVReader csvReader = new CSVReader(
        fileReader,
        config.getDelimiter(),
        CSVParser.DEFAULT_QUOTE_CHARACTER,
        CSVParser.NULL_CHARACTER);) {

      long line = 0;

      String[] headers = csvReader.readNext();
      line++;
      String[] fieldNames = constructFieldNames(headers, fileType, config.isCleanKeysForES());

      String[] record = null;
      while ((record = csvReader.readNext()) != null) {
        if (record.length != headers.length) {
          // unmatched record length
          LOGGER.warn("Unmatched record column count detected at line {} - header: {} record: {}",
              line, headers.length, record.length);
          continue;
        }
        LogRecord logRecord = new LogRecord();
        for (int i = 0; i < fieldNames.length; i++) {
          if (config.isSkipBlankFields() && StringUtils.isBlank(record[i])) {
            // skip
          } else {
            String value = config.isTrimFieldValue() ? StringUtils.trim(record[i]) : record[i];
            logRecord.setValue(fieldNames[i], value);
          }
        }
        collector.emit(logRecord);
      }
    }
  }

  String[] constructFieldNames(String[] headers, String fileType, boolean cleanKeysForES) {
    String[] fieldNames = new String[headers.length];
    for (int i = 0; i < headers.length; i++) {
      String fieldName = constructFieldName(fileType, i, headers[i]);
      fieldNames[i] = cleanKeysForES ? ElasticsearchUtil.cleanKey(fieldName) : fieldName;
    }
    return fieldNames;
  }

  String constructFieldName(String fileType, int column, String header) {
    // chose 5 zero's because Excel 2010 supports up to 16384 columns
    return String.format("%s_%05d_%s", fileType, column, header);
  }

  public interface LogRecordCollector {
    void emit(LogRecord logRecord);
  }
}
