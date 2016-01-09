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

import com.boozallen.cognition.ingest.storm.ConfigurationException;
import com.boozallen.cognition.ingest.storm.bolt.AbstractProcessingBolt;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Level;
import org.slf4j.Logger;

/**
 * Provides child class access to configurable logging
 *
 * @author bentse
 */
public abstract class AbstractLoggingBolt extends AbstractProcessingBolt {
  public static final String LEVEL = "level";
  protected Level level;

  @Override
  public void configure(Configuration conf) throws ConfigurationException {
    level = Level.toLevel(conf.getString(LEVEL), Level.DEBUG);
  }

  protected void log(Logger logger, String message) {
    switch (level.toInt()) {
      case Level.TRACE_INT:
        logger.trace(message);
        break;
      case Level.DEBUG_INT:
        logger.debug(message);
        break;
      case Level.INFO_INT:
        logger.info(message);
        break;
      case Level.WARN_INT:
        logger.warn(message);
        break;
      case Level.ERROR_INT:
        logger.error(message);
        break;
      default:
        logger.debug(message);
    }
  }
}
