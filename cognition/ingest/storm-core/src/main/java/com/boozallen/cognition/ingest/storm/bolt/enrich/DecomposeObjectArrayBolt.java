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
import com.google.gson.Gson;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Extract field values from an object array, creates new array of those values.
 * <p>
 * Example:
 * <p>
 * <code>twitter_entities.hashtags</code> is an object array with indices and text field in each object:
 * <code>[{"indices":[41,49],"text":"college"},{"indices":[50,55],"text":"poor"}]</code>
 * <p>
 * The following configuraion extracts text field from each object and creates new array containing:
 * <code>["college","poor"]</code>
 * <p>
 * <pre>
 * {@code
 *
 * <conf>
 *   <array-field>twitter_entities.hashtags</array-field>
 *   <object-field>text</object-field>
 *   <dest-field>twitter_entities.hashtags.text</dest-field>
 * </conf>
 * } </pre>
 *
 * @author bentse
 */
public class DecomposeObjectArrayBolt extends AbstractProcessingBolt {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public static final String ARRAY_FIELD = "array-field";
  public static final String OBJECT_FIELD = "object-field";
  public static final String DEST_FIELD = "dest-field";

  String arrayField;
  String objectField;
  String destField;

  @Override
  public void configure(Configuration conf) {
    arrayField = conf.getString(ARRAY_FIELD);
    objectField = conf.getString(OBJECT_FIELD);
    destField = conf.getString(DEST_FIELD);

    Validate.notBlank(arrayField);
    Validate.notBlank(objectField);
    Validate.notBlank(destField);
  }


  @Override
  protected void process(LogRecord record) {
    String arrayJson = record.getValue(arrayField);
    Gson gson = new Gson();
    List list = gson.fromJson(arrayJson, List.class);
    List decomposeList = new ArrayList<>();

    if(list != null) {
      for (Object object : list) {
        Map map = (Map) object;
        decomposeList.add(map.get(objectField));
      }
    }

    String json = new Gson().toJson(decomposeList);
    record.setValue(destField, json);
  }

}
