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

package com.boozallen.cognition.ingest.storm.topology;

/**
 * This is an abstract superclass for all ingest topologies. It contains common methods and final definitions for
 * default settings and Strings for Storm components used by topologies.
 */
public final class ConfigurableIngestTopologyConstants {


  // Storm keywords
  public static final String TOPOLOGY = "topology";
  public static final String STORM_CONF = "storm-conf";
  public static final String SPOUT = "spout";

  public static final String BOLTS = "bolts";
  public static final String BOLT = "bolt";
  static final String NUMBER_ATTRIBUTE = "[@number]"; // bolt number
  public static final String SUBSCRIBE_TO_BOLT = "subscribeToBolt";

  public static final String TYPE = "type";
  public static final String CONF = "conf";

  // Topology settings
  protected static final String TOPOLOGY_NAME = "name";
  protected static final String DO_LOCAL_SUBMIT = "doLocalSubmit";
  protected static final boolean DO_LOCAL_SUBMIT_DEFAULT = false;

  // Bolt settings
  protected static final String PARALLELISM_HINT = "parallelismHint";
  protected static final int PARALLELISM_HINT_DEFAULT = 1;
  protected static final String NUM_TASKS = "numTasks";
  protected static final String STREAM_GROUPING_CONF_TYPE = "streamGrouping";
  protected static final String STREAM_ID = "streamId";
  protected static final String STREAM_GROUPING_CONF_ARGS = "streamGroupingArgs";

  // Steam Types
  protected static final String STREAM_GROUPING_FIELDS = "Fields";
  protected static final String STREAM_GROUPING_LOCAL_OR_SHUFFLE = "LocalOrShuffle";
  protected static final String STREAM_GROUPING_SHUFFLE = "Shuffle";

}
