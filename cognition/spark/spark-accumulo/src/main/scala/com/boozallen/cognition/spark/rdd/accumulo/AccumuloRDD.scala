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

package com.boozallen.cognition.spark.rdd.accumulo

import org.apache.accumulo.core.client.mapreduce.{AccumuloInputFormat, AccumuloRowInputFormat}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.util.PeekingIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{NewHadoopRDD, RDD}

import scala.collection.JavaConversions._

class AccumuloRDD(sc: SparkContext, @transient conf: Configuration) extends NewHadoopRDD[Key, Value](sc, classOf[AccumuloInputFormat], classOf[Key], classOf[Value], conf) {

  def getAccumuloKeyValueRDD() = {
    sc.newAPIHadoopRDD(conf,
      classOf[AccumuloInputFormat], classOf[Key], classOf[Value]);
  }

  def getAccumuloRowRDD() = {
    sc.newAPIHadoopRDD(conf,
      classOf[AccumuloRowInputFormat], classOf[Text], classOf[PeekingIterator[java.util.Map.Entry[Key, Value]]]);
  }

  def asRowMap(rdd: RDD[(Text, PeekingIterator[java.util.Map.Entry[Key, Value]])]) = {
    rdd.map(kv => (kv._1,
      kv._2.map(x =>
        ((x.getKey().getColumnFamily().toString, x.getKey().getColumnQualifier.toString),
          new String(x.getValue.get()))).toMap[(String, String), String]))
  }

  //writes are turned off for the time being as they are not sufficiently tested
  /*	def writeMutations(pair: PairRDDFunctions[Text,Mutation]){
    pair.saveAsNewAPIHadoopFile("-", classOf[Text], classOf[Mutation], classOf[AccumuloOutputFormat], conf)
	}

	def writeMutations(tableName: String, rdd: RDD[Mutation]){
    val tableText = sc.broadcast(new Text(tableName))
    val pair = new PairRDDFunctions(rdd.map(mutation => (tableText.value,mutation)))
    pair.saveAsNewAPIHadoopFile("-", classOf[Text], classOf[Mutation], classOf[AccumuloOutputFormat], conf)
	}*/
}