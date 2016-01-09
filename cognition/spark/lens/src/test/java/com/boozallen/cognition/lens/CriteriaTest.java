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

package com.boozallen.cognition.lens;

import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.junit.Assert.*;

public class CriteriaTest {
  @Test
  public void doTest() {
    Criteria criteria = new Criteria();

    // Test default value
    assertEquals("", criteria.getAccumuloTable());
    assertEquals(Instant.MIN, criteria.getDateStart());
    assertEquals(Instant.MAX, criteria.getDateEnd());
    assertNotNull(criteria.getStringMatches());
    assertNotNull(criteria.getKeywords());
    assertNull(criteria.getSchema());

    // Now set the config to have some values
    criteria.setAccumuloTable("TableName");
    criteria.setCaseSensitive(true);
    criteria.setDates(Instant.parse("2015-12-25T00:00:00.00Z"), Instant.parse("2015-12-25T23:59:59.00Z"));
    criteria.addKeyword("Key1");
    criteria.addKeyword("Key2");
    criteria.addKeywords(Arrays.asList("Key3", "Key4"));
    criteria.setSource("Twitter");
    criteria.addMatch(Field.JSON, "json");
    criteria.addMatch(Field.KEYWORD, "keyword");
    criteria.addMatch(Field.LANGUAGE, "language");
    criteria.addMatch(Field.LOCATION, "location");
    criteria.addMatch(Field.USER, "user");
    criteria.useSpaceTokens(false);
    criteria.setSchema(new SchemaAdapter());

    // Get back and check the values
    assertEquals("TableName", criteria.getAccumuloTable());
    assertEquals(Instant.parse("2015-12-25T00:00:00.00Z"), criteria.getDateStart());
    assertEquals(Instant.parse("2015-12-25T23:59:59.00Z"), criteria.getDateEnd());
    assertEquals(4, criteria.getKeywords().size());
    assertEquals(5, criteria.getStringMatches().size());
    assertEquals("json", criteria.getStringMatches().get(Field.JSON));
    assertEquals("keyword", criteria.getStringMatches().get(Field.KEYWORD));
    assertEquals("language", criteria.getStringMatches().get(Field.LANGUAGE));
    assertEquals("location", criteria.getStringMatches().get(Field.LOCATION));
    assertEquals("user", criteria.getStringMatches().get(Field.USER));
    assertEquals(false, criteria.getUseSpaceTokens());
    assertEquals(true, criteria.isCaseSensitive());
    assertNotNull(criteria.getSchema());
  }
}
