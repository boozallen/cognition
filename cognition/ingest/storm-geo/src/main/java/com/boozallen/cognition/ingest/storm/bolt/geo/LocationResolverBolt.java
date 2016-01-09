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

import com.bericotech.clavin.GeoParser;
import com.bericotech.clavin.GeoParserFactory;
import com.bericotech.clavin.gazetteer.GeoName;
import com.bericotech.clavin.nerd.StanfordExtractor;
import com.bericotech.clavin.resolver.ResolvedLocation;
import com.boozallen.cognition.ingest.storm.bolt.AbstractProcessingBolt;
import com.boozallen.cognition.ingest.storm.ConfigurationException;
import com.boozallen.cognition.ingest.storm.util.HdfsFileReader;
import com.boozallen.cognition.ingest.storm.vo.LogRecord;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This bolt attempts to resolve location(s) in a specified location field of a LogRecord.
 * <p>
 * Alternatively, it looks at the list of unresolvedLocations of a LogRecord and attempts to resolve.
 * Depends on a Lucene index directory stored either on the local file system
 * or HDFS (if path specified in xml conf starts with "hdfs://")
 *
 * @author michaelkorb
 * @update hwu
 */
public class LocationResolverBolt extends AbstractProcessingBolt {
  private static final long serialVersionUID = 4178385668646268871L;
  private static final Logger logger = LoggerFactory.getLogger(LocationResolverBolt.class);

  private static final String LUCENE_INDEX_DIR = "luceneIndexDir";
  private static final String HADOOP_CONFIG = "hadoop-config";
  private static final String LOCAL_INDEX_DIR = "localIndexDir"; //if copying file from HDFS, where to store locally
  private static final String TEXT_FIELDS = "textFields";

  //Stuff that needs to be added to the existing config files
  private static final String PIP_CLAVIN_LOCATION_PREFIX = "clavinLocationPrefix";
  private static final String FIELD_NAME = "fieldName";
  private static final String NAME = "name";
  private static final String ADMIN1CODE = "admin1Code";
  private static final String ADMIN2CODE = "admin2Code";
  private static final String COUNTRYCODE = "countryCode";
  private static final String LATITUDE = "latitude";
  private static final String LONGITUDE = "longitude";
  private static final String CONFIDENCE = "confidence";

  //Stuff that actually IS static
  private static final String HDFS_PREFIX = "hdfs://";
  private static GeoParser _parser;

  private String _luceneIndexDir;
  private String _localIndexDir;
  private List<Object> _textFields; //fields to geoparse
  private String _pipClavinLocationPrefix;
  private String _fieldName;
  private String _name;
  private String _admin1Code;
  private String _admin2Code;
  private String _countryCode;
  private String _latitude;
  private String _longitude;
  private String _confidence;
  private HdfsFileReader hdfsFileReader;
  private Map<String, String> hadoopConfig = new HashMap<>();

  @Override
  public void configure(Configuration conf) throws ConfigurationException {
    _luceneIndexDir = conf.getString(LUCENE_INDEX_DIR);
    _localIndexDir = conf.getString(LOCAL_INDEX_DIR);
    _textFields = conf.getList(TEXT_FIELDS);
    _pipClavinLocationPrefix = conf.getString(PIP_CLAVIN_LOCATION_PREFIX, "pip.clavinLocation_");
    _fieldName = conf.getString(FIELD_NAME, FIELD_NAME);
    _name = conf.getString(NAME, NAME);
    _admin1Code = conf.getString(ADMIN1CODE, ADMIN1CODE);
    _admin2Code = conf.getString(ADMIN2CODE, ADMIN2CODE);
    _countryCode = conf.getString(COUNTRYCODE, COUNTRYCODE);
    _latitude = conf.getString(LATITUDE, LATITUDE);
    _longitude = conf.getString(LONGITUDE, LONGITUDE);
    _confidence = conf.getString(CONFIDENCE, CONFIDENCE);

    Configuration hadoopConfigSubset = conf.subset(HADOOP_CONFIG);
    for (Iterator<String> itr = hadoopConfigSubset.getKeys(); itr.hasNext(); ) {
      String key = itr.next();
      String value = hadoopConfigSubset.getString(key);
      hadoopConfig.put(key, value);
    }
  }

  @Override
  protected void process(LogRecord record) {
    if (_parser == null) {
      this.prepare();
    }

    int count = 0;
    for (Object field : _textFields) {
      String text = record.getValue((String) field);
      if (StringUtils.isBlank(text)) continue;
      try {
        List<ResolvedLocation> resolvedLocations = _parser.parse(text);
        for (ResolvedLocation rl : resolvedLocations) {
          GeoName geo = rl.geoname;

          record.setValue(_pipClavinLocationPrefix + count + "." + _name, geo.name);
          if (geo.admin1Code != null && !geo.admin1Code.isEmpty()) {
            record.setValue(_pipClavinLocationPrefix + count + "." + _admin1Code, geo.admin1Code); //US state or FIPS code
          }
          if (geo.admin2Code != null && !geo.admin2Code.isEmpty()) {
            record.setValue(_pipClavinLocationPrefix + count + "." + _admin2Code, geo.admin2Code); //county
          }
          record.setValue(_pipClavinLocationPrefix + count + "." + _countryCode, geo.primaryCountryCode.toString());
          record.setValue(_pipClavinLocationPrefix + count + "." + _latitude, String.valueOf(geo.latitude));
          record.setValue(_pipClavinLocationPrefix + count + "." + _longitude, String.valueOf(geo.longitude));
          record.setValue(_pipClavinLocationPrefix + count + "." + _confidence, String.valueOf(rl.confidence));
          record.setValue(_pipClavinLocationPrefix + count + "." + _fieldName, (String) field);
          count++;
        }
      } catch (Exception e) {
        logger.error("Failed to geoparse text: " + text);
      }
    }
  }

  private void prepare() {
    if (hdfsFileReader == null) {
      hdfsFileReader = new HdfsFileReader(hadoopConfig);
    }

    File luceneIndexDir;
    if (_luceneIndexDir.toLowerCase().startsWith(HDFS_PREFIX)) { // get file from Hdfs
      luceneIndexDir = new File(_localIndexDir);
      if (!luceneIndexDir.exists()) {
        String hdfsPath = _luceneIndexDir.substring(HDFS_PREFIX.length());
        logger.info("Getting " + hdfsPath + " from HDFS and writing to " + luceneIndexDir.getAbsolutePath());
        try {
          hdfsFileReader.getFileFromHdfs(hdfsPath, luceneIndexDir);
        } catch (IllegalArgumentException | IOException e) {
          logger.error("Unable to get file from HDFS: " + hdfsPath, e);
        }
      }
    } else { // use local directory
      luceneIndexDir = new File(_luceneIndexDir);
    }

    logger.info("Local Lucene index for CLAVIN: " + luceneIndexDir.getAbsolutePath());
    initializeStaticGeoParser(luceneIndexDir);
  }

  private static synchronized void initializeStaticGeoParser(File luceneIndexDir) {
    if (_parser == null) {
      try {
        _parser = GeoParserFactory.getDefault(luceneIndexDir.getAbsolutePath(), new StanfordExtractor(), 1, 1, false);
        logger.info("Successfully initialized CLAVIN GeoParser with index directory " + luceneIndexDir.getAbsolutePath());
      } catch (Exception e) {
        logger.error("Unable to initialize GeoParser.", e);
      }
    }
  }

}
