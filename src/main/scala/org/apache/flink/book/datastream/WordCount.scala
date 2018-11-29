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

package org.apache.flink.book.datastream

import org.apache.flink.book.connectors.SourceFunctions
import org.apache.flink.book.functions.FlatMapFunctions.TokenizerFlatMap
import org.apache.flink.book.connectors.SinkFunctions.PrintSinkFunction
import org.apache.flink.book.connectors.SourceFunctions.WordCountSourceFunction
import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    // Streaming 环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //方便我们查出输出数据
    env.setParallelism(1)

    // 获得DataStream
    val dataStream = env.addSource(new WordCountSourceFunction)
    // 分词
    val tokenizerWord = dataStream.flatMap(new TokenizerFlatMap)
    //分组计数统计
    val result = tokenizerWord
      // 初始化计数统计二元组
      .map{(_, 1)}
      // 分组统计
      .keyBy(0)
      .sum(1)

    // 将结果插入sink
    result.addSink(new PrintSinkFunction[(String, Int)])
    env.execute()
  }
}
