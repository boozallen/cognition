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

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
 * Wraps the query methods in an object so that they can be broken apart but still properly serialized by spark.
 */
object QueryFilter {
  /**
   * Filters the rdd based on the provided criteria
   */
  def query(rdd: RDD[(Text, scala.collection.immutable.Map[(String, String), String])], criteria: Criteria): RDD[(Text, scala.collection.immutable.Map[(String, String), String])] = {
    rdd.filter(row => matchJava(row._2, criteria))
  }

  private def matchJava(rowMap: scala.collection.immutable.Map[(String, String), String], criteria: Criteria) = {
    CriteriaMatcher.evaluate(criteria, rowMap)
  }

  /**
   * Builds up all columns to request from accumulo based on the provided criteria.
   */
  def getColumns(criteria: Criteria) = {
    var columns = criteria.getStringMatches.keySet.map(x =>
      criteria.getSchema.getColumns(x))
      .flatMap(x => x).map(x => (x.getColumnFamily.toString(), x.getColumnQualifier.toString()))

    if (criteria.getKeywords != null && criteria.getKeywords.size() > 0) {
      columns ++= criteria.getSchema.getColumns(Field.KEYWORD).map(x => (x.getColumnFamily.toString(), x.getColumnQualifier.toString()))
    }

    columns += criteria.getSchema.getTuples(Field.JSON).get(0)

    columns.toSeq
  }

}