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

package com.boozallen.cognition.ingest.storm.util;

import org.apache.commons.configuration.Configuration;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for reading {@link Configuration}
 */
public class ConfigurationMapEntryUtils {

  /**
   * Extracts map entries from configuration.
   *
   * @param conf
   * @param mappingEntry
   * @param fields       first field should be unique and exist in all entries, other fields are optional from map
   * @return
   */
  public static Map<String, Map<String, String>> extractMapList(
      final Configuration conf,
      final String mappingEntry,
      final String... fields) {
    String keyField = fields[0];
    List<Object> keys = conf.getList(mappingEntry + "." + keyField);
    Map<String, Map<String, String>> maps = new HashMap<>(keys.size());

    for (int i = 0; i < keys.size(); i++) {
      Map<String, String> map = new HashMap<>();
      Object key = keys.get(i);
      map.put(keyField, key.toString());

      for (int j = 1; j < fields.length; j++) {
        String field = fields[j];
        String fieldPath = String.format("%s(%s).%s", mappingEntry, i, field);
        String value = conf.getString(fieldPath);
        if (value != null)
          map.put(field, value);
      }

      maps.put(key.toString(), map);
    }
    return maps;
  }

  /**
   * Extracts Map&lt;String, String&gt; from XML of the format:
   * <pre>
   * {@code <mapName>
   *     <entry>
   *       <key>k0</key>
   *       <value>v0</value>
   *     </entry>
   *     <entry>
   *       <key>k1</key>
   *       <value>v1</value>
   *     </entry>
   *   </mapName>
   * }
   * </pre>
   *
   * @param conf
   * @param mapName
   * @param entry
   * @param key
   * @param value
   * @return
   */
  public static Map<String, String> extractSimpleMap(final Configuration conf,
                                                     final String mapName, final String entry, final String key, final String value) {
    List<Object> fieldsList = conf.getList(mapName + "." + entry + "." + key);
    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();
    for (int i = 0; i < fieldsList.size(); i++) {
      String relevanceField = conf.getString(mapName + "." + entry + "(" + i + ")." + value);
      map.put(fieldsList.get(i).toString(), relevanceField);
    }
    return map;
  }
}
