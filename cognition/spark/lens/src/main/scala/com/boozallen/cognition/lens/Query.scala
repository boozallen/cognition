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

package com.boozallen.cognition.lens

import com.boozallen.cognition.accumulo.config.CognitionConfiguration
import com.boozallen.cognition.spark.accumulo.accumulo._
import com.boozallen.cognition.spark.common.utils._
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class that runs the query and assembles the results into json
 */
class Query(sc: SparkContext, val criteria: Criteria, cognitionConfiguration: CognitionConfiguration) extends Serializable {
  val config = AccumuloAdapter.getAccumuloConfiguration(criteria, cognitionConfiguration)
  config.fetchColumns(QueryFilter.getColumns(criteria));
  protected[this] val unfilteredRDD = sc.accumuloExpandedRowRDD(config.getConfiguration())

  /**
   * Returns the RDD object of the result without the action having occured on it
   */
  def query(): RDD[(Text, scala.collection.immutable.Map[(String, String), String])] = {
    QueryFilter.query(unfilteredRDD, criteria)
  }

  /**
   * Calls the collect action on the query filtered RDD and assembles into json
   */
  def json(): String = query.makeJson(jsonField = criteria.getSchema.getTuples(Field.JSON, criteria.getSource).get(0));

   /**
   * Calls the take(limit) action on the query filtered RDD and assembles into json
   */
  //can't use default parameters when calling from java -- for java compatibility
  def json(limit: Int): String = query.makeJson(limit, criteria.getSchema.getTuples(Field.JSON, criteria.getSource).get(0));

}