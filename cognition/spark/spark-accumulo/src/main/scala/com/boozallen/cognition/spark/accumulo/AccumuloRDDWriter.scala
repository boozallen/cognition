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

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

private[accumulo] class AccumuloRDDWriter(@transient conf: Configuration) {

  def writeMutations(pair: PairRDDFunctions[Text, Mutation]) {
    pair.saveAsNewAPIHadoopFile("-", classOf[Text], classOf[Mutation], classOf[AccumuloOutputFormat], conf)
  }

  def writeMutations(mutations: RDD[Mutation], tableName: String) {
    val pair = mutations.map(mutation => (new Text(tableName), mutation))
    writeMutations(pair);
  }

  def writeToAccumulo[U](rdd: RDD[U], tableName: String, mutationTranformation: RDD[U] => RDD[Mutation]) {
    val pair = mutationTranformation(rdd).map(mutation => (new Text(tableName), mutation))
    writeMutations(pair);
  }

  def writeToAccumulo(rdd: RDD[Tuple2[Key, Value]], tableName: String) {
    writeToAccumulo[Tuple2[Key, Value]](rdd, tableName, keyValueMutationTranformation)
  }

  def keyValueMutationTranformation(rdd: RDD[Tuple2[Key, Value]]): RDD[Mutation] = {
    rdd.keyBy(kv => kv._1.getRow()).groupByKey().map(rowIdKVPair =>
      makeMutation(rowIdKVPair._1, rowIdKVPair._2))
  }

  def makeMutation(rowId: Text, pairs: Iterable[Tuple2[Key, Value]]): Mutation = {
    val mutation = new Mutation(rowId);
    pairs.foreach(kv => mutation.put(kv._1.getColumnFamily(), kv._1.getColumnQualifier(), kv._2))
    return mutation;
  }

  def mutationTranform(triple: Tuple2[String, Long]): Mutation = {

    val mutation = new Mutation(triple._1)
    mutation.put(triple._2.toString, "", new Value("".getBytes()))
    return mutation

  }

  def mutationTranform(triple: Tuple3[String, String, String]): Mutation = {

    val mutation = new Mutation(triple._1)
    mutation.put(triple._2, triple._3, new Value("".getBytes()))
    return mutation

  }

  /*	def writeMutations(tableName: String, rdd: RDD[Mutation]){
    val tableText = sc.broadcast(new Text(tableName))
    val pair = new PairRDDFunctions(rdd.map(mutation => (tableText.value,mutation)))
    pair.saveAsNewAPIHadoopFile("-", classOf[Text], classOf[Mutation], classOf[AccumuloOutputFormat], conf)
}*/
}