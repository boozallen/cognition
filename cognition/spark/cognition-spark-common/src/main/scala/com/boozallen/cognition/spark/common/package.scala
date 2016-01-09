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

package com.boozallen.cognition.spark.common

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD

package object utils {
  val DefaultJsonField = ("data", "cognition.esjson")

  /**
   * Separate each object in the array with a comma and wrap with brackets. This is for
   * creating a json object array.
   */
  def collapseArray(arr: Array[String]): String = "[" + arr.mkString(",") + "]"

  /**
   * Add this functionality to the RDD existing libraries when this package is imported
   */
  implicit def cognitionRDDFunctions(rdd: RDD[(Text, scala.collection.immutable.Map[(String, String), String])]) =
    new CognitionRDDFunctions(rdd)

  class CognitionRDDFunctions(rdd: RDD[(Text, scala.collection.immutable.Map[(String, String), String])]) extends Serializable {

    /**
     * Get the json of the particular source identified by jsonField and collapse it into a large json array
     * without deserializing and then serializing. 
     * 
     * Caution: This wraps a spark collect action.
     * 
     * @param limit -- the number of results to return. If limit is -1, return all results.
     * @param jsonField -- the field in the datasource where the json is stored
     */
    def makeJson(limit: Int = -1, jsonField: Tuple2[String, String] = DefaultJsonField): String = {
      def jsonCognitionRDD(jsonField: Tuple2[String, String]): RDD[String] =
        rdd.filter(_._2.toMap.contains(jsonField))
          .map(x => x._2).map(x => x.get(jsonField).get)

      try {
        if (limit < 0) {
          collapseArray(jsonCognitionRDD(jsonField).collect())
        } else {
          collapseArray(jsonCognitionRDD(jsonField).take(limit))
        }
      } catch {
        case e: java.lang.UnsupportedOperationException => "[]"
      }
    }
  }

}