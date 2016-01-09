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

import org.junit.BeforeClass;
import org.junit.Test;

import com.boozallen.cognition.accumulo.structure.Source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SchemaTest {
  private static SchemaAdapter gnip;
  private static SchemaAdapter datasift;

  @BeforeClass
  public static void initMoreover() {
    gnip = new SchemaAdapter();

    gnip.addField(Field.USER, new Column(Source.TWITTER, "data", "actor.displayName"));
    gnip.addField(Field.LOCATION, new Column(Source.TWITTER, "data", "cognition.location"));
    gnip.addField(Field.LOCATION, new Column(Source.TWITTER, "data", "cognition.location.country"));
    gnip.addField(Field.LANGUAGE, new Column(Source.TWITTER, "data", "gnip.language.value"));
    gnip.addField(Field.KEYWORD, new Column(Source.TWITTER, "data", "body"));
    gnip.addField(Field.JSON, new Column(Source.TWITTER, "data", "cognition.esjson"));
    //gnip.addField(Field.JSON, new Column(Source.FACEBOOK,"data", "cognition.esjson"));

  }

  @BeforeClass
  public static void initGnip() {
    datasift = new SchemaAdapter();

    datasift.addField(Field.USER, new Column(Source.TWITTER, "twitter.user.name", "twitter"));
    datasift.addField(Field.USER, new Column(Source.FACEBOOK, "facebook.author.name", "facebook"));

    datasift.addField(Field.LOCATION, new Column(Source.TWITTER, "pip.location", "twitter"));
    datasift.addField(Field.LOCATION, new Column(Source.TWITTER, "pip.location.country", "twitter"));
    datasift.addField(Field.LOCATION, new Column(Source.FACEBOOK, "pip.location.country", "twitter"));
    datasift.addField(Field.LOCATION, new Column(Source.FACEBOOK, "pip.location", "twitter"));

    datasift.addField(Field.LANGUAGE, new Column(Source.TWITTER, "twitter.lang", "twitter"));
    datasift.addField(Field.LANGUAGE, new Column(Source.TWITTER, "language.tag", "twitter"));

    datasift.addField(Field.KEYWORD, new Column(Source.TWITTER, "interaction.content", "twitter"));
    datasift.addField(Field.KEYWORD, new Column(Source.FACEBOOK, "interaction.content", "facebook"));

    datasift.addField(Field.JSON, new Column(Source.TWITTER, "pip.esjson", "twitter"));
    datasift.addField(Field.JSON, new Column(Source.FACEBOOK, "pip.esjson", "facebook"));

  }

  @Test
  public void testLoad() {
    SchemaAdapter s = new SchemaAdapter();
    s.loadJson("moreover-schema.json");
    assertEquals("moreover", s.getTableName());
  }

  @Test
  public void testGnip() {
    // Schema1 from setup
    String json = gnip.getJson();
    //System.out.println(json);
    String adapted = json.replace("body", "test");

    // Schema2 from file
    SchemaAdapter s = new SchemaAdapter();
    s.loadJsonString(adapted);
    SchemaAdapter s2 = new SchemaAdapter();
    s2.loadJson("gnip-schema.json");

    // Compare the two
    //System.out.println(s2.getJson());
    assertNotEquals(s.getColumns(Field.KEYWORD, Source.TWITTER).get(0).getColumnQualifier(),
        s2.getColumns(Field.KEYWORD, Source.TWITTER).get(0).getColumnQualifier());
    assertEquals(s.getColumns(Field.LANGUAGE, Source.TWITTER).get(0).getColumnQualifier(),
        s2.getColumns(Field.LANGUAGE, Source.TWITTER).get(0).getColumnQualifier());
    assertEquals(s.getColumns(Field.LOCATION, Source.TWITTER).get(0).getColumnQualifier(),
        s2.getColumns(Field.LOCATION, Source.TWITTER).get(0).getColumnQualifier());
    assertEquals(s.getColumns(Field.LOCATION, Source.TWITTER).get(1).getColumnQualifier(),
        s2.getColumns(Field.LOCATION, Source.TWITTER).get(1).getColumnQualifier());
    assertEquals(s.getColumns(Field.USER, Source.TWITTER).get(0).getColumnQualifier(),
        s2.getColumns(Field.USER, Source.TWITTER).get(0).getColumnQualifier());
    assertEquals(s.getColumns(Field.JSON, Source.TWITTER).get(0).getColumnQualifier(),
        s2.getColumns(Field.JSON, Source.TWITTER).get(0).getColumnQualifier());
  }

  @Test
  public void testDataSift() {
    String json = datasift.getJson();
    System.out.println(json);
  }

}
