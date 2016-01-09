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

import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Tuple;
import com.boozallen.cognition.ingest.storm.bolt.AbstractLogRecordBolt;
import com.boozallen.cognition.ingest.storm.bolt.csv.CsvLogRecordParser;
import com.boozallen.cognition.ingest.storm.bolt.csv.CsvLogRecordParserConfig;
import com.boozallen.cognition.ingest.storm.vo.FileMetadata;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

import static org.apache.commons.lang.StringUtils.isBlank;

/**
 * Head of bolt chain. Receives CSV URL info from spout, fetch file and creates {@link LogRecord} from each CSV entries
 * for downstream processing.
 */
public class CsvUrlBolt extends AbstractLogRecordBolt {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvUrlBolt.class);

  private CsvLogRecordParserConfig csvParserConfig;

  @Override
  public void configure(Configuration conf) {
    csvParserConfig = new CsvLogRecordParserConfig(conf);
  }

  @Override
  protected void execute(Tuple tuple, RecordCollector collector) {
    String metadataJson = new String((byte[]) tuple.getValue(0));
    File tempFile = null;

    try {
      FileMetadata fileMetadata = FileMetadata.parseJson(metadataJson);

      String filename = fileMetadata.getFilename();
      String fileUrl = fileMetadata.getFileUrl();
      String fileType = fileMetadata.getFileType();

      if (isBlank(filename) || isBlank(fileUrl) || isBlank(fileType)) {
        LOGGER.error("Incomplete file metadata. Requires: filename, fileUrl and fileType. {}", fileMetadata);
        throw new FailedException("Incomplete file metadata: " + fileMetadata);
      }

      tempFile = File.createTempFile("csv", null);
      FileUtils.copyURLToFile(new URL(fileUrl), tempFile);

      try (FileReader fileReader = new FileReader(tempFile);) {

        CsvLogRecordParser parser = new CsvLogRecordParser(csvParserConfig);
        parser.parse(fileReader, fileType, logRecord -> {
          logRecord.setValue("filename", filename);
          logRecord.setValue("fileType", fileType);
          logRecord.setValue("fileUrl", fileUrl);
          collector.emit(logRecord);
        });
      }
    } catch (IOException e) {
      LOGGER.error(metadataJson, e);
      throw new FailedException(e);
    } finally {
      FileUtils.deleteQuietly(tempFile);
    }
  }

}
