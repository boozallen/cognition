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

import com.boozallen.cognition.ingest.storm.util.IngestUtilities;
import org.apache.commons.configuration.Configuration;

import java.io.Serializable;

/**
 * Holds configurations for {@link CsvLogRecordParser}
 */
public class CsvLogRecordParserConfig implements Serializable {
  public static final String DEFAULT_DELIMITER = ",";
  public static final String CLEAN_KEYS_FOR_ES = "cleanKeysForES";
  public static final String SKIP_BLANK_FIELDS = "skipBlankFields";
  public static final String TRIM_FIELD_VALUE = "trimFieldValue";
  public static final String DELIMITER = "delimiter";

  private final boolean cleanKeysForES;
  private final boolean skipBlankFields;
  private final boolean trimFieldValue;
  private final char delimiter;

  public CsvLogRecordParserConfig(Configuration conf) {
    delimiter = IngestUtilities.getDelimiterByName(conf.getString(DELIMITER, DEFAULT_DELIMITER));
    cleanKeysForES = conf.getBoolean(CLEAN_KEYS_FOR_ES, true);
    skipBlankFields = conf.getBoolean(SKIP_BLANK_FIELDS, true);
    trimFieldValue = conf.getBoolean(TRIM_FIELD_VALUE, true);
  }

  public boolean isCleanKeysForES() {
    return cleanKeysForES;
  }

  public boolean isSkipBlankFields() {
    return skipBlankFields;
  }

  public boolean isTrimFieldValue() {
    return trimFieldValue;
  }

  public char getDelimiter() {
    return delimiter;
  }
}
