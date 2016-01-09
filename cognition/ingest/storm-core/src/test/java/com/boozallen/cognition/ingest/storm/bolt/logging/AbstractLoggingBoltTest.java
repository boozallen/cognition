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

package com.boozallen.cognition.ingest.storm.bolt.logging;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;
import org.apache.log4j.Level;
import org.junit.Test;
import org.slf4j.Logger;

public class AbstractLoggingBoltTest {
  @Tested
  AbstractLoggingBolt bolt;

  @Test
  public void testLogTrace(@Injectable Logger logger) throws Exception {
    bolt.level = Level.TRACE;
    new Expectations() {{
      logger.trace("msg");

    }};
    bolt.log(logger, "msg");
  }

  @Test
  public void testLogDebug(@Injectable Logger logger) throws Exception {
    bolt.level = Level.DEBUG;
    new Expectations() {{
      logger.debug("msg");

    }};
    bolt.log(logger, "msg");
  }

  @Test
  public void testLogInfo(@Injectable Logger logger) throws Exception {
    bolt.level = Level.INFO;
    new Expectations() {{
      logger.info("msg");

    }};
    bolt.log(logger, "msg");
  }

  @Test
  public void testLogWarn(@Injectable Logger logger) throws Exception {
    bolt.level = Level.WARN;
    new Expectations() {{
      logger.warn("msg");

    }};
    bolt.log(logger, "msg");
  }

  @Test
  public void testLogError(@Injectable Logger logger) throws Exception {
    bolt.level = Level.ERROR;
    new Expectations() {{
      logger.error("msg");
    }};
    bolt.log(logger, "msg");
  }

  @Test
  public void testLogDefault(@Injectable Logger logger) throws Exception {
    bolt.level = Level.FATAL;
    new Expectations() {{
      logger.debug("msg");
    }};
    bolt.log(logger, "msg");
  }
}