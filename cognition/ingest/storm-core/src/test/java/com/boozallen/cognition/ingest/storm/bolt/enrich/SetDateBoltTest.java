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

import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Tested;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author bentse
 */
public class SetDateBoltTest {

  @Tested
  SetDateBolt bolt;

  @Test
  public void testConfigure(@Injectable Configuration conf) {
    new Expectations() {{
      conf.getList(SetDateBolt.DATE_FIELD);
      result = Arrays.asList("field0");
      conf.getString(SetDateBolt.DATE_FORMAT);
      result = "format0";
    }};
    bolt.configure(conf);
    assertThat(bolt.dateFields.get(0), is("field0"));
    assertThat(bolt.dateFormat, is("format0"));
  }


  @Test
  public void testEnrichDataswift(@Mocked final LogRecord record) throws Exception {
    final Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    calendar.set(2014, Calendar.JUNE, 10, 20, 22, 4);

    bolt.dateFields = Arrays.asList("interaction.created_at");
    bolt.dateFormat = "EEE, dd MMM yyyy HH:mm:ss Z";

    new Expectations() {{
      record.getValue(bolt.dateFields.get(0));
      result = "Tue, 10 Jun 2014 20:22:04 +0000";
      record.setDate(calendar.getTime());
    }};
    bolt.process(record);
  }

  @Test
  public void testEnrichGnip(@Mocked final LogRecord record) throws Exception {
    final Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
    calendar.set(2015, Calendar.JULY, 12, 0, 0, 52);

    bolt.dateFields = Arrays.asList("postedTime");
    bolt.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSX";

    new Expectations() {{
      record.getValue(bolt.dateFields.get(0));
      result = "2015-07-12T00:00:52.000Z";
      record.setDate(calendar.getTime());
    }};
    bolt.process(record);
  }
}