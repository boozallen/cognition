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

package com.boozallen.cognition.spark.accumulo

import java.util.ArrayList

import com.boozallen.cognition.accumulo.config.AccumuloConfiguration
import org.apache.accumulo.core.data.Mutation
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions

package object accumulo {
  
  /**
   * Add SparkContextFunctions to sc api.
   */
  implicit def sparkContextFunctions(sc: SparkContext) = new SparkContextFunctions(sc)

  class SparkContextFunctions(sc: SparkContext) extends Serializable {
    /** Returns RDD[(Key, Value)] */
    def accumuloRDD(conf: Configuration) = AccumuloSpark.accumuloRDD(sc, conf)

    /** Returns RDD[(Text, PeekingIterator[Map.Entry[Key, Value]])] */
    def accumuloRowRDD(conf: Configuration) = AccumuloSpark.accumuloRowRDD(sc, conf)

    /** Returns RDD[(Text, Map[(String, String), String])] */
    def accumuloExpandedRowRDD(conf: Configuration) = AccumuloSpark.accumuloExpanedRowRDD(sc, conf)
  }

  implicit def sparkRDDFunctions(pairs: PairRDDFunctions[Text, Mutation]) = new SparkRDDFunctions(pairs)

  class SparkRDDFunctions(pairs: PairRDDFunctions[Text, Mutation]) extends Serializable {
    def writeToAccumulo(conf: Configuration) {
      AccumuloSpark.writeToAccumulo(pairs, conf)
    }
  }

  implicit def accumuloConfig(config: AccumuloConfiguration) = new AccumuloConfig(config)

  class AccumuloConfig(config: AccumuloConfiguration) extends Serializable {

    /**
     * Convenience method for converting Seq[(String,String)] to the necessary format
     * for column pruning
     */
    def fetchColumns(pairs: Seq[Tuple2[String, String]]) {
      val list = new ArrayList[org.apache.accumulo.core.util.Pair[Text, Text]];
      pairs.foreach(pair => {
        val cfText = new Text(pair._1)
        val cqText = if (pair._2 != null) new Text(pair._2) else null;
        list.add(new org.apache.accumulo.core.util.Pair(cfText, cqText))
      })
      config.fetchColumns(list)
    }
  }

}
