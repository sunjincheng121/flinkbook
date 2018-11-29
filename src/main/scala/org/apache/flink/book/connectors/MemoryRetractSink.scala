/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.book.connectors

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.book.utils.FileUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.sinks._
import org.apache.flink.types.Row

import scala.collection.mutable

class MemoryRetractSink extends RetractStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _

  override def getRecordType: TypeInformation[Row] = new RowTypeInfo(fTypes, fNames)

  override def emitDataStream(s: DataStream[JTuple2[JBool, Row]]): Unit = {
    s.addSink(new RichSinkFunction[JTuple2[JBool, Row]] {
      private var resultSet: mutable.Set[Row] = _

      override def open(parameters: Configuration): Unit = {
        resultSet = new mutable.HashSet[Row]
      }

      override def invoke(v: JTuple2[JBool, Row], context: SinkFunction.Context[_]): Unit =
        RowCollector.addValue(v)

      override def close(): Unit = {
        val results = RowCollector.retractResults()
        results.foreach(println)
        val path = FileUtils.writeToTempFile(
          results.mkString(System.getProperty("line.separator")),
          "csv_sink_", "tmp")
        println("---------------------------------")
        println("You can run [ cat " + path + " ]")
      }
    })
  }

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def configure(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]]): TableSink[JTuple2[JBool, Row]] = {
    val copy = new MemoryRetractSink
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy
  }
}

object RowCollector {
  private val sink: mutable.ArrayBuffer[JTuple2[JBool, Row]] =
    new mutable.ArrayBuffer[JTuple2[JBool, Row]]()

  def addValue(value: JTuple2[JBool, Row]): Unit = {
    // make a copy
    val copy = new JTuple2[JBool, Row](value.f0, Row.copy(value.f1))
    sink.synchronized {
      sink += copy
    }
  }

  /** Converts a list of retraction messages into a list of final results. */
  def retractResults(): List[String] = {
    val results = sink.toList
    sink.clear()
    val retracted =
      results
      .foldLeft(Map[String, Int]()) { (m: Map[String, Int], v: JTuple2[JBool, Row]) =>
        val cnt = m.getOrElse(v.f1.toString, 0)
        if (v.f0) {
          m + (v.f1.toString -> (cnt + 1))
        } else {
          m + (v.f1.toString -> (cnt - 1))
        }
      }.filter { case (_, c: Int) => c != 0 }

    retracted.flatMap { case (r: String, c: Int) => (0 until c).map(_ => r) }.toList
  }

}
