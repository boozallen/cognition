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

import mockit.Injectable;
import mockit.Tested;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CsvLogRecordParserTest {
  @Tested
  CsvLogRecordParser parser;
  @Injectable
  CsvLogRecordParserConfig config;

  @Test
  public void testConstructFieldNames() {
    assertThat(parser.constructFieldNames(new String[]{"field-meh"}, "type", true)[0], is("type_00000_field_meh"));
  }

  @Test
  public void testConstructFieldName() {
    assertThat(parser.constructFieldName("type", 1, "field"), is("type_00001_field"));
    assertThat(parser.constructFieldName("type", 1234, "field"), is("type_01234_field"));
    assertThat(parser.constructFieldName("type", 123456, "field"), is("type_123456_field"));
  }
}