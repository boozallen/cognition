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
import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;

import java.util.Calendar;
import java.util.TimeZone;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author bentse
 */
public class LogRecordDateBoltTest {

  @Tested
  LogRecordDateBolt bolt;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConfigure(@Injectable Configuration conf) throws ConfigurationException {
    new Expectations(bolt) {{
      conf.getString(LogRecordDateBolt.LEVEL);
      result = "info";
      conf.getString(LogRecordDateBolt.DATE_FORMAT, anyString);
      result = "format0";
      bolt.validateDateFormat();
    }};
    bolt.configure(conf);
    assertThat(bolt.dateFormat, is("format0"));
    assertThat(bolt.level, is(Level.INFO));
  }

  @Test
  public void testValidateDateFormat() throws Exception {
    bolt.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    bolt.validateDateFormat();
  }

  @Test
  public void testValidateDateFormatFail() throws Exception {
    bolt.dateFormat = "yyyy-MM-ddTHH:mm:ss.SSSXXX";
    thrown.expect(ConfigurationException.class);
    bolt.validateDateFormat();
  }

  @Test
  public void testProcessSample(@Injectable LogRecord record) {
    new Expectations(bolt) {{
      bolt.shouldSample();
      result = true;
      bolt.logDate(record);
    }};
    bolt.process(record);
  }

  @Test
  public void testProcessNotSample(@Injectable LogRecord record) {
    new Expectations(bolt) {{
      bolt.shouldSample();
      result = false;
      bolt.logDate(record);
      times = 0;
    }};
    bolt.process(record);
  }

  @Test
  public void testLogDate(@Injectable LogRecord record,
                          @Injectable Logger logger) throws Exception {
    bolt.logger = logger;
    bolt.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    bolt.level = Level.DEBUG;

    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    calendar.set(2014, Calendar.JUNE, 10, 20, 22, 4);

    new Expectations(bolt) {{
      record.getDate();
      result = calendar.getTime();
      bolt.log(logger, "2014-06-10T20:22:04.000Z");
    }};
    bolt.logDate(record);
  }

  @Test
  public void testLogDateFail(@Injectable LogRecord record,
                              @Injectable Logger logger) throws Exception {
    bolt.logger = logger;
    bolt.level = Level.DEBUG;

    new Expectations(bolt) {{
      record.getDate();
      result = null;

      logger.error(anyString);
      bolt.log(logger, anyString);
      times = 0;
    }};
    bolt.logDate(record);
  }
}