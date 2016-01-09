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
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds metadata field and value to record
 * <p>
 * <p>
 * Example adding data type metadata to record:
 * <pre>
 * {@code
 *
 * <conf>
 *   <field>cognition.dataType</field>
 *   <value>twitter</value>
 * </conf>
 * } </pre>
 *
 * @author bentse
 */
public class AddMetadataBolt extends AbstractProcessingBolt {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  String field;
  String value;

  @Override
  public void configure(Configuration conf) {
    field = conf.getString("field");
    value = conf.getString("value");

    Validate.notBlank(field);
    Validate.notBlank(value);
  }

  @Override
  protected void process(LogRecord record) {
    record.addMetadataValue(field, value);
  }
}
