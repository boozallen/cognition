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

package com.boozallen.cognition.endpoint.applications;

import com.boozallen.cognition.endpoint.configurations.DataConfiguration;
import com.boozallen.cognition.endpoint.health.ServiceHealthCheck;
import com.boozallen.cognition.endpoint.resources.JsonLensEndpoint;
import com.boozallen.cognition.endpoint.resources.LensEndpoint;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pulls together the various bundles and commands to provide basic functionality
 * for data web service.
 */
public class DataService extends Service<DataConfiguration> {

  final static Logger LOGGER = LoggerFactory.getLogger(DataService.class);

  // Main
  public static void main(String[] args) {
    try {
      new DataService().run(args);
    } catch (Exception e) {
      LOGGER.error("Issue(s) running application. ", e);
    }
  }

  /**
   * Configures aspects of the application required before the
   * application is run.
   */
  @Override
  public void initialize(Bootstrap<DataConfiguration> bootstrap) {
    bootstrap.setName("Lens Service");
  }

  /**
   * Establishes an environment and configures resources need to
   * execute service. Optional service health checks.
   */
  @Override
  public void run(DataConfiguration configuration, Environment environment) {
    final LensEndpoint restResource = new LensEndpoint();
    environment.addResource(restResource);
    final JsonLensEndpoint jsonResource = new JsonLensEndpoint();
    environment.addResource(jsonResource);

    // Application complains if health check not included. Can add more robust
    // service tests later for more complete code base.
    environment.addHealthCheck(new ServiceHealthCheck());
  }

}
