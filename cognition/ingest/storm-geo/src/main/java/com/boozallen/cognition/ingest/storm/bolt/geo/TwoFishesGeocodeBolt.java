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

package com.boozallen.cognition.ingest.storm.bolt.geo;

import com.boozallen.cognition.ingest.storm.bolt.AbstractProcessingBolt;
import com.boozallen.cognition.ingest.storm.ConfigurationException;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Uses a Twofishes server to resolve locations in a LogRecord.
 *
 * @author michaelkorb
 * @update hwu
 */
public class TwoFishesGeocodeBolt extends AbstractProcessingBolt {
  private static final long serialVersionUID = -4702888008441911691L;
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private static final String SERVER = "server";
  private static final String QUERY = "/?responseIncludes=PARENTS&query=";
  private static final String LAT_LON_QUERY = "/?responseIncludes=PARENTS&ll=";
  private static final String PIP_LOCATION = "cognition.location";
  private static final String LOCATION_FIELDS = "locationFields";
  private static final String FIELD_NAME = "fieldName";
  private static final String CC = "cc";
  private static final String LAT = "lat";
  private static final String LNG = "lng";
  public static final String COORDINATES = "coordinates";
  public static final String COORDINATES_FIELD = "coordinatesField";
  private static final String USE_MULTIPLE_LOCATIONS = "useMultipleLocations";

  static final Map<Long, String> WOE_TYPES = ImmutableMap.of(
      7L, "city",
      8L, "state",
      9L, "county"
  );

  private String _server;
  private String coordinatesField;
  private List<String> _locationFields; //first non-blank field in this list will be resolved and added
  private int _successCount;
  private int _failCount;
  private int _exceptionCount;
  private boolean _useMultipleLocations;

  @Override
  public void configure(Configuration conf) throws ConfigurationException {
    _server = conf.getString(SERVER);
    coordinatesField = conf.getString(COORDINATES_FIELD, "geo.coordinates");
    _locationFields = new ArrayList<>();
    conf.getList(LOCATION_FIELDS).forEach(x -> _locationFields.add(x.toString()));
    _successCount = 0;
    _failCount = 0;
    _exceptionCount = 0;
    _useMultipleLocations = conf.getBoolean(USE_MULTIPLE_LOCATIONS, false);
  }

  @Override
  protected void process(LogRecord record) {
    //resolve location fields
    String coordinates = record.getValue(coordinatesField);

    Gson gson = new Gson();
    List list = gson.fromJson(coordinates, List.class);

    String lat = null;
    String lon = null;
    if (CollectionUtils.isNotEmpty(list)) {
      lat = list.get(0).toString();
      lon = list.get(1).toString();
    }

    if (lat != null && lon != null) {
      if (resolveLocation(LAT_LON_QUERY, lat + "," + lon, record)) { //only add source field name if successful
        record.setValue(PIP_LOCATION + "." + COORDINATES + "." + FIELD_NAME, coordinatesField);
      }
    } else {
      if (_useMultipleLocations) {
        String startField = "";
        String query = "";
        for (String field : _locationFields) {
          String value = record.getValue(field);
          if (StringUtils.isBlank(value)) {
            continue;
          }
          if (StringUtils.isEmpty(startField)) {
            startField = field;
          }
          if (StringUtils.isEmpty(query)) {
            query = value;
          } else {
            query += "," + value;
            // 2 level of locations is good enough
            break;
          }
        }
        if (StringUtils.isNotEmpty(query)) {
          if (resolveLocation(QUERY, query, record)) {
            record.setValue(PIP_LOCATION + "." + FIELD_NAME, startField);
          }
        }
      } else {
        for (String field : _locationFields) {
          String value = record.getValue(field);
          if (StringUtils.isBlank(value)) continue;
          if (resolveLocation(QUERY, value, record)) {
            record.setValue(PIP_LOCATION + "." + FIELD_NAME, field);
            break; // done as soon as we resolve a location
          }
        }
      }
    }
  }

  /**
   * Queries twofishes server for unresolvedlocation an adds result to record
   *
   * @param unresolvedLocation
   * @param record
   */
  private boolean resolveLocation(String queryPrefix, String unresolvedLocation, LogRecord record) {
    if (unresolvedLocation.startsWith("http")) { //prevent twofishes "java.lang.Exception: don't support url queries"
      return false;
    }

    //print metrics every 1000 records
    long total;
    if ((total = _successCount + _failCount + _exceptionCount) % 1000 == 0) {
      logger.debug("Total records: " + total);
      logger.debug("Successes: " + _successCount);
      logger.debug("Failures: " + _failCount);
      logger.debug("Exceptions: " + _exceptionCount);
      logger.debug("Success percentage: " + 100 * (double) _successCount / total);
    }

    try {
      String urlEncodedQuery = URLEncoder.encode(unresolvedLocation, "UTF-8");
      TwoFishesFeature[] results = submitQuery(_server + queryPrefix + urlEncodedQuery);
      if (results != null) {
        //results for one location order from most precise to least
        //e.g. Chicago, Cook County, Illinois, United States
        for (int i = 0; i < results.length; i++) {
          TwoFishesFeature result = results[i];
          if (i == 0) { //only save CC and lat/lng of precise location
            record.setValue(PIP_LOCATION, result.countryCode); //for legacy support
            record.setValue(PIP_LOCATION + ".country", result.countryCode);
            Gson gson = new Gson();
            double lat = Double.parseDouble(result.lat);
            double lng = Double.parseDouble(result.lng);
            String json = gson.toJson(Arrays.asList(lat, lng));
            record.setValue(PIP_LOCATION + "." + COORDINATES, json);
          }
          String woeType = WOE_TYPES.get(result.woeType);
          if (woeType != null) { //ignore unknown woetypes
            record.setValue(PIP_LOCATION + "." + woeType, result.name);
          }
        }


        return true;
      }
    } catch (Exception e) {
      logger.info("Exception while attempting to resolve location: \"" + unresolvedLocation + "\".", e);
      _exceptionCount++;
    }
    return false;
  }

  /**
   * Submits a URL query string and builds a TwoFishesFeature for the result and all parents.
   *
   * @param query
   * @return
   * @throws IOException
   * @throws ParseException
   */
  private TwoFishesFeature[] submitQuery(String query) throws IOException, ParseException {
    URL url = new URL(query);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("Accept", "application/json");
    if (conn.getResponseCode() != 200) {
      logger.error("Failed : HTTP error code : " + conn.getResponseCode() + ":" + url.toString());
      _failCount++;
      return null;
    }
    BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
    String jsonStr = br.readLine();
    br.close();
    conn.disconnect();

    JSONParser parser = new JSONParser();
    JSONObject jsonObject = (JSONObject) parser.parse(jsonStr);
    JSONArray interpretations = (JSONArray) jsonObject.get("interpretations");

    if (interpretations.size() == 0) {
      //logger.warn("Twofishes unable to resolve location " + unresolvedLocation);
      _failCount++;
      return null;
    } else {
      if (interpretations.size() > 1) {
        //lat-long searches have multiple interpretations, e.g. city, county, state, country. most specific is first, so still use it.
        logger.debug("Twofishes has more than one interpretation of " + query);
      }
      JSONObject interpretation0 = (JSONObject) interpretations.get(0);

      //ArrayList of main result and parents
      ArrayList<JSONObject> features = new ArrayList<JSONObject>();
      JSONObject feature = (JSONObject) interpretation0.get("feature");

      //save main result
      features.add(feature);

      JSONArray parents = (JSONArray) interpretation0.get("parents");
      //get parents as feature JSONObjects
      for (int i = 0; i < parents.size(); i++) {
        JSONObject parentFeature = (JSONObject) parents.get(i);
        features.add(parentFeature);
      }

      //parse features and return
      TwoFishesFeature[] results = new TwoFishesFeature[features.size()];

      for (int i = 0; i < features.size(); i++) {
        JSONObject f = features.get(i);
        TwoFishesFeature result = new TwoFishesFeature();
        result.name = (String) f.get("name");
        result.countryCode = (String) f.get(CC);
        result.woeType = (long) f.get("woeType");
        JSONObject geometry = (JSONObject) f.get("geometry");
        JSONObject center = (JSONObject) geometry.get("center");
        result.lat = String.valueOf(center.get(LAT));
        result.lng = String.valueOf(center.get(LNG));
        results[i] = result;
      }

      _successCount++;
      return results;
    }

  }

  private class TwoFishesFeature {
    public String name;
    public String countryCode;
    public String lat;
    public String lng;
    public long woeType;
  }

}
