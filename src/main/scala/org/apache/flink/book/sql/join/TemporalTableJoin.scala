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
package org.apache.flink.book.sql.join

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.book.connectors.{CsvTableSourceUtils, MemoryRetractSink}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TemporalTableJoin {
  def main(args: Array[String]): Unit = {
    // Streaming 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tEnv = TableEnvironment.getTableEnvironment(env)

    //方便我们查出输出数据
    env.setParallelism(1)
    val rateSourceTableName = "RatesHistory"
    // 创建CSV rates source数据结构
    val tableSource = CsvTableSourceUtils.genEventRatesHistorySource
    // 注册source
    tEnv.registerTableSource(rateSourceTableName, tableSource)

    // 创建Row的TypeInformation,因为Row的各个字段无法自动推导出具体数据类型，必须用new的方式创建RowTypeInfo
    implicit val t :  TypeInformation[Row] = new RowTypeInfo(Types.LONG, Types.STRING, Types.LONG)

    val tab = tEnv.scan(rateSourceTableName).toAppendStream[Row]
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[Row] {
        override def checkAndGetNextWatermark(
          t: Row,
          l: Long): Watermark = new Watermark(l) // 根据ts字段创建WaterMark
        override def extractTimestamp(t: Row, l: Long): Long = {
          t.getField(0).asInstanceOf[Long] // 利用ts字段作为EventTime
        }
      }).toTable(tEnv, 'ts, 'currency, 'rate, 'rowtime.rowtime)

    val ratesFunction = tab.createTemporalTableFunction('rowtime, 'currency)
    tEnv.registerFunction("Rates", ratesFunction)

    val orderSourceTableName = "orders"
    // 创建CSV order source数据结构
    val orderTableSource = CsvTableSourceUtils.genRatesOrderSource
    // 注册source
    tEnv.registerTableSource(orderSourceTableName, orderTableSource)

    val orderTab = tEnv.scan(orderSourceTableName).toAppendStream[Row]
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[Row] {
      override def checkAndGetNextWatermark(
        t: Row,
        l: Long): Watermark = new Watermark(l) // 根据ts字段创建WaterMark
      override def extractTimestamp(t: Row, l: Long): Long = {
        t.getField(0).asInstanceOf[Long] // 利用ts字段作为EventTime
      }
    }).toTable(tEnv, 'ts, 'currency, 'amount, 'rowtime.rowtime)

    tEnv.registerTable("orderTab", orderTab)

    // 注册retract sink
    val sinkTableName = "retractSink"
    val fieldNames = Array("ts", "currency", "amount", "yen_amount")
    val fieldTypes: Array[TypeInformation[_]] =
      Array(Types.LONG, Types.STRING, Types.LONG, Types.LONG)

    tEnv.registerTableSink(
      sinkTableName,
      fieldNames,
      fieldTypes,
      new MemoryRetractSink)

    val sql =
      """
        |SELECT o.ts, o.currency, o.amount,
        |  o.amount * r.rate AS yen_amount
        |FROM
        |  orderTab AS o,
        |  LATERAL TABLE (Rates(o.rowtime)) AS r
        |WHERE r.currency = o.currency
        |""".stripMargin

    // 执行查询
    val result = tEnv.sqlQuery(sql)

    // 将结果插入sink
    result.insertInto(sinkTableName)
    env.execute()
  }

}
