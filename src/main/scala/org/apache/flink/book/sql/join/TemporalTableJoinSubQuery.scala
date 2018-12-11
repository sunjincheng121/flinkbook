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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.book.connectors.{CsvTableSourceUtils, MemoryRetractSink}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, Types}

object TemporalTableJoinSubQuery {
  def main(args: Array[String]): Unit = {
    // Streaming 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    //方便我们查出输出数据
    env.setParallelism(1)

    val sourceTableName = "RatesHistory"
    // 创建CSV source数据结构
    val tableSource = CsvTableSourceUtils.genRatesHistorySource
    // 注册source
    tEnv.registerTableSource(sourceTableName, tableSource)

    // 注册retract sink
    val sinkTableName = "retractSink"
    val fieldNames = Array("rowtime", "currency", "rate")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING, Types.STRING)

    tEnv.registerTableSink(
      sinkTableName,
      fieldNames,
      fieldTypes,
      new MemoryRetractSink)

    val sql =
      """
        |SELECT *
        |FROM RatesHistory AS r
        |WHERE r.rowtime = (
        |  SELECT MAX(rowtime)
        |  FROM RatesHistory AS r2
        |  WHERE r2.currency = r.currency
        |  AND r2.rowtime <= '10:58:00'  )
      """.stripMargin

    // 执行查询
    val result = tEnv.sqlQuery(sql)

    // 将结果插入sink
    result.insertInto(sinkTableName)
    env.execute()
  }

}
