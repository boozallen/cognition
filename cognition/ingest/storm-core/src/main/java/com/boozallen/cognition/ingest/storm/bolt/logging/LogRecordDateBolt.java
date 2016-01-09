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

package com.boozallen.cognition.ingest.storm.bolt.logging;

import com.boozallen.cognition.ingest.storm.ConfigurationException;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

/**
 * Logs record date to specified log level.
 * <p>
 * Example sampling 0.1% of records, logging at info level
 * <pre>
 * {@code
 *
 * <conf>
 *   <parallelismHint>3</parallelismHint>
 *   <sample>0.001</sample> <!-- 0.1% -->
 *   <level>info</level>
 *   <date-format>yyyy-MM-dd'T'HH:mm:ss.SSSX</date-format>
 * </conf>
 * } </pre>
 *
 * @author bentse
 */
public class LogRecordDateBolt extends AbstractLoggingBolt {
  Logger logger = LoggerFactory.getLogger(this.getClass());

  public static final String DATE_FORMAT = "date-format";
  public static final String SAMPLE = "sample";

  String dateFormat;
  double sample;

  @Override
  public void configure(Configuration conf) throws ConfigurationException {
    super.configure(conf);

    dateFormat = conf.getString(DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    sample = conf.getDouble(SAMPLE, 0.01);

    Validate.notBlank(dateFormat);

    validateDateFormat();
  }

  boolean shouldSample() {
    double dice = RandomUtils.nextDouble(0, 1);
    return dice <= sample;
  }

  void validateDateFormat() throws ConfigurationException {
    try {
      new SimpleDateFormat(dateFormat);
    } catch (IllegalArgumentException e) {
      logger.error("Bad date format {}", dateFormat);
      throw new ConfigurationException(e);
    }
  }

  @Override
  protected void process(LogRecord record) {
    if (shouldSample()) {
      logDate(record);
    }
  }

  void logDate(LogRecord record) {
    Date date = record.getDate();
    if (date == null) {
      logger.error("Record date is null!");
    } else {
      SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
      sdf.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
      super.log(logger, sdf.format(date));
    }
  }
}
