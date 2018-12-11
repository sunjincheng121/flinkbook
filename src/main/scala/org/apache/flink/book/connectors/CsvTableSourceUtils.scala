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

import java.io.File

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.book.utils.{CommonUtils, FileUtils}
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

object CsvTableSourceUtils {

  def genWordCountSource: CsvTableSource = {
    val csvRecords = Seq(
      "words",
      "Hello Flink",
      "Hi, Apache Flink",
      "Apache FlinkBook"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      FileUtils.writeToTempFile(csvRecords.mkString("$"), "csv_source_", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("words"),
      Array(
        Types.STRING
      ),
      fieldDelim = "#",
      rowDelim = "$",
      ignoreFirstLine = true,
      ignoreComments = "%"
    )
  }


  def genRatesHistorySource: CsvTableSource = {

    val csvRecords = Seq(
      "rowtime ,currency   ,rate",
    "09:00:00   ,US Dollar  , 102",
    "09:00:00   ,Euro       , 114",
    "09:00:00  ,Yen        ,   1",
    "10:45:00   ,Euro       , 116",
    "11:15:00   ,Euro       , 119",
    "11:49:00   ,Pounds     , 108"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      FileUtils.writeToTempFile(csvRecords.mkString("$"), "csv_source_", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("rowtime","currency","rate"),
      Array(
        Types.STRING,Types.STRING,Types.STRING
      ),
      fieldDelim = ",",
      rowDelim = "$",
      ignoreFirstLine = true,
      ignoreComments = "%"
    )
  }

  def genEventRatesHistorySource: CsvTableSource = {

    val csvRecords = Seq(
      "ts#currency#rate",
      "1#US Dollar#102",
      "1#Euro#114",
      "1#Yen#1",
      "3#Euro#116",
      "5#Euro#119",
      "7#Pounds#108"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      FileUtils.writeToTempFile(csvRecords.mkString(CommonUtils.line), "csv_source_rate", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("ts","currency","rate"),
      Array(
        Types.LONG,Types.STRING,Types.LONG
      ),
      fieldDelim = "#",
      rowDelim = CommonUtils.line,
      ignoreFirstLine = true,
      ignoreComments = "%"
    )
  }

  def genRatesOrderSource: CsvTableSource = {

    val csvRecords = Seq(
      "ts#currency#amount",
      "2#Euro#10",
      "4#Euro#10"
    )
    // 测试数据写入临时文件
    val tempFilePath =
      FileUtils.writeToTempFile(csvRecords.mkString(CommonUtils.line), "csv_source_order", "tmp")

    // 创建Source connector
    new CsvTableSource(
      tempFilePath,
      Array("ts","currency", "amount"),
      Array(
        Types.LONG,Types.STRING,Types.LONG
      ),
      fieldDelim = "#",
      rowDelim = CommonUtils.line,
      ignoreFirstLine = true,
      ignoreComments = "%"
    )
  }


  /**
    * Example:
    * genCsvSink(
    *   Array[String]("word", "count"),
    *   Array[TypeInformation[_] ](Types.STRING, Types.LONG))
    */
  def genCsvSink(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[Row] = {
    val tempFile = File.createTempFile("csv_sink_", "tem")
    if (tempFile.exists()) {
      tempFile.delete()
    }
    new CsvTableSink(tempFile.getAbsolutePath).configure(fieldNames, fieldTypes)
  }

}
