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

package com.boozallen.cognition.endpoint.resources;

import com.boozallen.cognition.accumulo.structure.Source;
import com.boozallen.cognition.lens.Criteria;
import com.boozallen.cognition.lens.Field;
import com.boozallen.cognition.lens.LensAPI;
import com.boozallen.cognition.lens.SchemaAdapter;
import com.sun.jersey.api.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Resource class used to implement service methods and associate the
 * appropriate URI template.
 *
 */

// Use of SparkDataService is deprecated and will be removed in future versions
@Path("/{a:lens|SparkDataService}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LensEndpoint {

  final static Logger LOGGER = LoggerFactory.getLogger(LensEndpoint.class);

  private LensAPI api;

  private final static String QUERY_ERROR = "Failed to execute query: ";
  private final static String FAILURE = "Failed to execute query.\n";

  public LensEndpoint() {
    api = new LensAPI();
  }

  @GET
  public String defaultBehavior(@QueryParam("user") String user, @QueryParam("keywords") List<String> keywords,
      @QueryParam("language") String language, @QueryParam("country") String country,
      @NotNull @QueryParam("startDate") String startDate, @NotNull @QueryParam("endDate") String endDate,
      @QueryParam("source") String source,
      @DefaultValue("true") @QueryParam("useSpaceTokenization") boolean useSpaceTokenization,
      @QueryParam("limit") int limit) {
    return query(user, keywords, language, country, startDate, endDate, source, useSpaceTokenization, limit);
  }

  /**
   * Returns data bound to specific query parameters.
   * Required parameters: Start and end date.
   *
   * @return String The results from query in JSON form.
   */
  @GET
  @Path("query")
  public String query(@QueryParam("user") String user,
      @QueryParam("keywords") List<String> keywords,
      @QueryParam("language") String language,
      @QueryParam("country") String country,
      @NotNull @QueryParam("startDate") String startDate,
      @NotNull @QueryParam("endDate") String endDate,
      @QueryParam("source") String source,
      @DefaultValue("true") @QueryParam("useSpaceTokenization") boolean useSpaceTokenization,
      @QueryParam("limit") int limit) {


    if (startDate == null || endDate == null) {
      throw new IllegalArgumentException("Missing required date parameter(s).");
    }

    long start = System.currentTimeMillis();

    Instant startInstant = java.time.Instant.parse(startDate);
    Instant endInstant = java.time.Instant.parse(endDate);

    if (limit > 100000) { //# of millis in a day
      throw new NotFoundException("Please specify a limit less than 100,000.");
    }

    if (startInstant.until(endInstant, ChronoUnit.MILLIS) > 86400000 && limit <= 0) { //# of millis in a day
      throw new NotFoundException("The date range you specified is larger than a day. Please shrink your"
          + " time window or provide a limit.");
    }

    Criteria criteria = buildCriteria(user, keywords, language, country, source, useSpaceTokenization,
        startInstant, endInstant);

    String sparkMessage = null;
    try {
      sparkMessage = query(criteria, limit);
    } catch (DateTimeParseException e) {
      LOGGER.error(QUERY_ERROR + "Issue parsing provided dates, please check date format.", e);
      return FAILURE;
    } catch (Exception e) {
      LOGGER.error(QUERY_ERROR, e);
      return FAILURE;
    }

    long end = System.currentTimeMillis();
    LOGGER.info("PERF: Data Retrieval took: " + (end - start) + "ms");

    return sparkMessage;
  }

  private Criteria buildCriteria(String user, List<String> keywords, String language, String country, String source,
      boolean useSpaceTokenization, Instant startInstant, Instant endInstant) {
    Criteria criteria = new Criteria().setDates(startInstant, endInstant);
    if (user != null) {
      criteria.addMatch(Field.USER, user);
      LOGGER.trace("Added User: " + user);
    }
    if (keywords != null && keywords.size() > 0) {
      criteria.addKeywords(keywords);
      LOGGER.trace("Added Keyword(s): " + keywords);
    }
    if (language != null) {
      criteria.addMatch(Field.LANGUAGE, language);
      LOGGER.trace("Added Language:  " + language);
    }
    if (country != null) {
      criteria.addMatch(Field.LOCATION, country);
      LOGGER.trace("Added Location: " + country);
    }

    if (source != null) {
      criteria.setSource(source);
      LOGGER.trace("Added Source: " + source);
    } else {
      // default is already twitter in configuration, just here for clarity
      // or in case of changes
      criteria.setSource(Source.TWITTER);
      LOGGER.trace("Defaulting to TWITTER source");
    }

    criteria.useSpaceTokens(useSpaceTokenization); // default true

    SchemaAdapter schema = new SchemaAdapter();
    if (criteria.getSource().equals(Source.MOREOVER)) {
      schema.loadJson("moreover-schema.json");
    } else {
      schema.loadJson("gnip-schema.json");
    }
    criteria.setSchema(schema);

    LOGGER.info("Criteria: " + criteria.toString());

    return criteria;
  }

  private String query(Criteria criteria, int limit) {
    String sparkMessage = (limit > 0) ? api.query(criteria, limit) : api.query(criteria);
    return sparkMessage;
  }

}
