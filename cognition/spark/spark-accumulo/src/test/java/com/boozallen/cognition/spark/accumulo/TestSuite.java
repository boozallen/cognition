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

package com.boozallen.cognition.spark.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.junit.BeforeClass;

public class TestSuite {

  public static Instance instance = new MockInstance("test");
  private static final String user = "root";
  private static final String password = "";

  @BeforeClass
  public static void init() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
    Connector conn = instance.getConnector("root", new PasswordToken());
    conn.tableOperations().create("test");

    Mutation m1 = new Mutation("rowId1");
    m1.put("cf1", "cq1", new Value("value1".getBytes()));
    
    Mutation m2 = new Mutation("rowId2");
    m2.put("cf2", "cq2", new Value("value2".getBytes()));

    BatchWriter writer = conn.createBatchWriter("test", new BatchWriterConfig());
    writer.addMutation(m1);
    writer.addMutation(m2);
  }


}
