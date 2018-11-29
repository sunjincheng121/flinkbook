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

import org.apache.flink.book.utils.FileUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import scala.collection.mutable

object SinkFunctions {

  class PrintSinkFunction[T] extends RichSinkFunction[T] {
    private var resultSet: mutable.ArrayBuffer[T] = _


    override def open(parameters: Configuration): Unit = {
      resultSet = new mutable.ArrayBuffer[T]
    }

    override def close(): Unit = {
      super.close()
      println("-----------------------计算结果------------------")
      resultSet.foreach(println)
      val path = FileUtils.writeToTempFile(
        resultSet.mkString(System.getProperty("line.separator")),
        "print_sink_", "tmp")
      println("---------------------------------")
      println("You can run [ cat " + path + " ]")
    }

    override def invoke(
      value: T,
      context: SinkFunction.Context[_]): Unit = {
      resultSet.append(value)
    }
  }

}
