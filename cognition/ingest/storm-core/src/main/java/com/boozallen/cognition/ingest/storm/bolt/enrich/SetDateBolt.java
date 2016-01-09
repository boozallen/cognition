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

import com.boozallen.cognition.ingest.storm.bolt.AbstractProcessingBolt;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Sets record date from field value
 * <p>
 * Example setting record date from [published_date, harvest_date], using first non-empty value from list.
 * <pre>
 * {@code
 *
 * <conf>
 *   <date-field>published_date,harvest_date</date-field>
 *   <date-format>yyyy-MM-dd'T'HH:mm:ssX</date-format>
 *   <updateIndexField>true</updateIndexField>
 * </conf>
 * } </pre>
 *
 * @author bentse
 * @author hwu
 */
public class SetDateBolt extends AbstractProcessingBolt {

  private static final long serialVersionUID = 1L;

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public static final String DATE_FIELD = "date-field";
  public static final String DATE_FORMAT = "date-format";
  private static final String UPDATE_INDEX_FIELD = "updateIndexField";

  List<String> dateFields;
  String dateFormat;
  private boolean _updateIndexField;


  @Override
  public void configure(Configuration conf) {
    dateFormat = conf.getString(DATE_FORMAT);
    dateFields = new ArrayList<>();
    conf.getList(DATE_FIELD).forEach(x -> dateFields.add(x.toString()));

    _updateIndexField = conf.getBoolean(UPDATE_INDEX_FIELD, false);

    Validate.notEmpty(dateFields);
    Validate.notBlank(dateFormat);
  }

  @Override
  protected void process(LogRecord record) {
    SimpleDateFormat sourceSdf = new SimpleDateFormat(dateFormat);
    String dateValue = null;
    for (int i = 0; i < dateFields.size(); i++) {
      dateValue = record.getValue(dateFields.get(i));
      if (StringUtils.isNotBlank(dateValue)) {
        if (_updateIndexField && i != 0) {
          // copy the value in a secondary field into index field if it's empty
          record.setValue(dateFields.get(0), dateValue);
        }
        break;
      }
    }

    try {
      Date date = sourceSdf.parse(dateValue);
      record.setDate(date);
    } catch (ParseException e) {
      logger.error("Failed to parse timestamp: " + dateValue);
    }
  }
}
