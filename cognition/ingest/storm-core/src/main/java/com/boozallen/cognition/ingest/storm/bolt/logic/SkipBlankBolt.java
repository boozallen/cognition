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

package com.boozallen.cognition.ingest.storm.bolt.logic;

import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Stop processing of a record if the value of given field is null or blank.
 * <p>
 * Example skipping record with blank "postedTime" field:
 * <pre>
 * {@code
 *
 * <conf>
 *   <field>postedTime</field>
 * </conf>
 * } </pre>
 *
 * @author bentse
 */
public class SkipBlankBolt extends AbstractLogicBolt {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public static final String FIELD = "field";

  String field;

  @Override
  public void configure(Configuration conf) {
    field = conf.getString(FIELD);
    Validate.notBlank(field);
  }

  @Override
  protected boolean shouldEmit(LogRecord record) {
    String value = record.getValue(field);
    if (isBlank(value))
      return false;
    else
      return true;
  }
}
