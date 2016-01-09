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

package com.boozallen.cognition.ingest.storm.bolt.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.Map;

import static com.boozallen.cognition.ingest.storm.util.ConfigurationMapEntryUtils.extractMapList;
import static com.boozallen.cognition.test.utils.TestResourceUtils.getResource;

/**
 *
 */
public class ConfigurationMapEntryUtilsTest {
  @Test
  public void test() throws ConfigurationException {
    URL resource = getResource(this.getClass(), "ConfigurationMapEntryUtilsTest.xml");
    XMLConfiguration conf = new XMLConfiguration(resource);
    Map<String, Map<String, String>> map = extractMapList(conf, "fieldTypeMapping.entry", "fieldName", "fieldType", "dateFormat");
    Assert.assertEquals(3, map.size());
    System.out.println(map);
  }
}
