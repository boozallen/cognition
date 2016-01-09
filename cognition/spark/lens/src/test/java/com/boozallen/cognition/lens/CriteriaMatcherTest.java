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

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Ignore;
import org.junit.Test;

import com.boozallen.cognition.accumulo.structure.Source;

import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CriteriaMatcherTest {
  @Test
  public void testCriteriaMatcherException() {
    Criteria criteria = new Criteria().setSource(Source.TWITTER);
    criteria.useSpaceTokens(false);
    criteria.setCaseSensitive(true);

    criteria.addMatch(Field.USER, "twitterUser");

    // TestException
    Map<Tuple2<String, String>, String> observedValues = new HashMap<>();
    observedValues.put(new Tuple2<String, String>("twitter.user.name", "twitter"), "twitterUser");

    // This should throw an exception because we don't have a schema set yet!
    /*try {
			CriteriaMatcher.evaluate(criteria, observedValues);
		} catch (ConfigurationException e) {
			// If we are here, we are good.  
			return;
		}

		// Opps, something is wrong, we should not be here!
		fail("CriteriaMatcherTest.testCriteriaMatcherException: Should not be here!");*/
  }

  @Ignore
  @Test
  public void testCriteriaMatcher() throws ConfigurationException {
    Map<Tuple2<String, String>, String> observedValues = new HashMap<>();

    Criteria criteria = new Criteria().addKeyword("room").setSource(Source.TWITTER);
    criteria.useSpaceTokens(false);
    criteria.setCaseSensitive(true);

    criteria.addMatch(Field.USER, "twitterUser");
    criteria.addMatch(Field.LOCATION, "USA");
    criteria.addMatch(Field.LANGUAGE, "en");
    criteria.addMatch(Field.KEYWORD, "room");
    criteria.addMatch(Field.JSON, "{\"source\": \"TWITTER\"");

    SchemaAdapter schema = new SchemaAdapter();
    schema.loadJson("datasift-twitter-schema.json");
    criteria.setSchema(schema);

    // Test user lookup
    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("twitter.user.name", "twitter"), "twitterUser");
    assertTrue(CriteriaMatcher.evaluate(criteria, observedValues));

    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("twitter.user.name", "twitter"), "otherUser");
    assertFalse(CriteriaMatcher.evaluate(criteria, observedValues));

    // Test location lookup
    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("pip.location", "twitter"), "USA");
    assertTrue(CriteriaMatcher.evaluate(criteria, observedValues));

    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("pip.location.country", "twitter"), "USA");
    assertTrue(CriteriaMatcher.evaluate(criteria, observedValues));

    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("pip.location", "twitter"), "Mexico");
    assertFalse(CriteriaMatcher.evaluate(criteria, observedValues));

    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("pip.location.country", "twitter"), "Canada");
    assertFalse(CriteriaMatcher.evaluate(criteria, observedValues));

    // Test language lookup
    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("twitter.lang", "twitter"), "en");
    assertTrue(CriteriaMatcher.evaluate(criteria, observedValues));

    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("language.tag", "twitter"), "en");
    assertTrue(CriteriaMatcher.evaluate(criteria, observedValues));

    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("twitter.lang", "twitter"), "spanish");
    assertFalse(CriteriaMatcher.evaluate(criteria, observedValues));

    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("language.tag", "twitter"), "spanish");
    assertFalse(CriteriaMatcher.evaluate(criteria, observedValues));

    // Test keyword lookup
    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("interaction.content", "twitter"), "Bianca_Hdz15 .That room brings back memories");
    assertTrue(CriteriaMatcher.evaluate(criteria, observedValues));

    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("interaction.content", "twitter"), "This is not a string I am looking for");
    assertFalse(CriteriaMatcher.evaluate(criteria, observedValues));

    // Test JSON lookup
    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("data", "cognition.esjson"), "{\"source\": \"TWITTER\"");
    assertTrue(CriteriaMatcher.evaluate(criteria, observedValues));

    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("data", "cognition.esjson"), "{\"source\": \"FACEBOOK\"}");
    assertFalse(CriteriaMatcher.evaluate(criteria, observedValues));

    // Test Case Sensitive
    criteria.setCaseSensitive(false);
    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("data", "cognition.esjson"), "{\"source\": \"Twitter\"");
    assertTrue(CriteriaMatcher.evaluate(criteria, observedValues));

    criteria.setCaseSensitive(true);
    assertFalse(CriteriaMatcher.evaluate(criteria, observedValues));

    // Test useSpaceTokens
    criteria.useSpaceTokens(true);
    criteria.addKeyword("here");
    observedValues.clear();
    observedValues.put(new Tuple2<String, String>("interaction.content", "twitter"), "There are many ways to skin a cat");
    assertFalse(CriteriaMatcher.evaluate(criteria, observedValues));

    criteria.useSpaceTokens(false);
    assertTrue(CriteriaMatcher.evaluate(criteria, observedValues));
  }
}
