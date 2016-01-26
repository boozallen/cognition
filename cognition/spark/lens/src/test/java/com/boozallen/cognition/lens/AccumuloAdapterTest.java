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

import com.boozallen.cognition.accumulo.config.AccumuloConfiguration;
import com.boozallen.cognition.accumulo.config.CognitionConfiguration;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;

import static org.junit.Assert.*;

public class AccumuloAdapterTest {
  @Test
  public void testGetAccumuloConfiguration() throws AccumuloSecurityException, IOException {
    // Create Criteria
    Criteria criteria = new Criteria();
    criteria.setDates(Instant.parse("2015-02-28T00:00:00.00Z"), Instant.parse("2015-02-28T23:59:59.99Z"));

    SchemaAdapter schema = new SchemaAdapter();
    schema.loadJson("moreover-schema.json");
    criteria.setSchema(schema);

    // Create CognitionConfiguration
    AccumuloConfiguration accumuloConfig = new AccumuloConfiguration(new MockInstance("test"), "root", "", true);
    CognitionConfiguration cognitionConfig = new CognitionConfiguration(accumuloConfig);

    // Now, we can get the AccumuloConfiguration
    AccumuloConfiguration newAccumoloConfig = AccumuloAdapter.getAccumuloConfiguration(criteria, cognitionConfig);

    assertNotNull(newAccumoloConfig);
    assertNotNull(newAccumoloConfig.getConfiguration());
    assertNotNull(newAccumoloConfig.getJob());
    assertEquals("moreover",criteria.getAccumuloTable());
  }

}
