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

import com.boozallen.cognition.spark.rdd.accumulo.AccumuloRDD
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

object AccumuloSpark {
  def accumuloRDD(sc: SparkContext, conf: Configuration) = new AccumuloRDD(sc, conf).getAccumuloKeyValueRDD

  def accumuloRowRDD(sc: SparkContext, conf: Configuration) = new AccumuloRDD(sc, conf).getAccumuloRowRDD

  def accumuloExpanedRowRDD(sc: SparkContext, conf: Configuration) = {
    val accumulo = new AccumuloRDD(sc, conf)
    val rdd = accumulo.getAccumuloRowRDD
    accumulo.asRowMap(rdd)
  }

  def writeToAccumulo(pair: PairRDDFunctions[Text, Mutation], conf: Configuration) {
    new AccumuloRDDWriter(conf).writeMutations(pair)
  }

  def writeToAccumulo(rdd: RDD[Tuple2[Key, Value]], tableName: String, conf: Configuration) {
    new AccumuloRDDWriter(conf).writeToAccumulo(rdd, tableName)
  }

  def writeToAccumulo[U](rdd: RDD[U], tableName: String, mutationTranformation: RDD[U] => RDD[Mutation],
                         conf: Configuration) {
    new AccumuloRDDWriter(conf).writeToAccumulo(rdd, tableName, mutationTranformation)
  }


}