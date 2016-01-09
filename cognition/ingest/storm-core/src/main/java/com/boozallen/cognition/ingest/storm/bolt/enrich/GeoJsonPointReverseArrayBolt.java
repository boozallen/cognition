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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.boozallen.cognition.ingest.storm.util.ConfigurationMapEntryUtils.extractSimpleMap;

/**
 * Reverse incoming [lat, lon] arrays to [lon, lat] arrays, confirming to GeoJSON format expected by Elasticsearch.
 * (https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-geo-point-type.html#_lat_lon_as_array_5)
 */
public class GeoJsonPointReverseArrayBolt extends AbstractProcessingBolt {

  public static final String SRC_FIELD = "srcField";
  public static final String DEST_FIELD = "destField";

  Map<String, String> srcDestFieldMapping;

  @Override
  public void configure(Configuration conf) {
    srcDestFieldMapping = extractSimpleMap(conf, "fields", "field", SRC_FIELD, DEST_FIELD);
  }

  @Override
  protected void process(LogRecord record) {
    for (Map.Entry<String, String> entry : srcDestFieldMapping.entrySet()) {
      final String src = record.getValue(entry.getKey());

      String reversedArrayJson = reverseJsonArray(src);
      record.setValue(entry.getValue(), reversedArrayJson);
    }
  }

  String reverseJsonArray(String src) {
    Gson gson = new Gson();
    List list = gson.fromJson(src, List.class);

    if (CollectionUtils.isEmpty(list)) {
      return StringUtils.EMPTY;
    } else {
      Collections.reverse(list);
      return gson.toJson(list);
    }
  }

}
