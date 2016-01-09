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

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

import org.junit.Test
import org.junit.Assert._

import com.boozallen.cognition.spark.accumulo.accumulo._
import com.boozallen.cognition.accumulo.config.AccumuloConfiguration

import scala.collection.JavaConversions._

class AccumuloTest {
  @Test
  def accumuloRDDs() {
    TestSuite.init()
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    conf.setAppName("test");
    conf.setMaster("local[2]");

    val sc = new SparkContext(conf);

    val config = new AccumuloConfiguration(TestSuite.instance, "root", "", true)
    config.setTableName("test")

    val accumuloExpandedRow = sc.accumuloExpandedRowRDD(config.getConfiguration())
    val accumuloRow = sc.accumuloRowRDD(config.getConfiguration())
    val accumuloKV = sc.accumuloRDD(config.getConfiguration())

    val accumuloExpandedRowValue = accumuloExpandedRow.first
    val accumuloRowValue = accumuloRow.map(x => (x._1, x._2.toList)).first //trouble serializing peeking iterator
    val accumuloKVValue = accumuloKV.first

    assertEquals("rowId1", accumuloExpandedRowValue._1.toString())
    assertEquals("cf1", accumuloExpandedRowValue._2.toList(0)._1._1)
    assertEquals("cq1", accumuloExpandedRowValue._2.toList(0)._1._2)
    assertEquals("value1", accumuloExpandedRowValue._2.toList(0)._2.toString())

    assertEquals("rowId1", accumuloRowValue._1.toString())
    assertEquals("cf1", accumuloRowValue._2(0).getKey.getColumnFamily.toString())
    assertEquals("cq1", accumuloRowValue._2(0).getKey.getColumnQualifier.toString())
    assertEquals("value1", accumuloRowValue._2(0).getValue.toString())
    
    assertEquals("rowId1", accumuloKVValue._1.getRow().toString())
    assertEquals("cf1", accumuloKVValue._1.getColumnFamily.toString())
    assertEquals("cq1", accumuloKVValue._1.getColumnQualifier.toString())
    assertEquals("value1", accumuloKVValue._2.toString())

  }
}