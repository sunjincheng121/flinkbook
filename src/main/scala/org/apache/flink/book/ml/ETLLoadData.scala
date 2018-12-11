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

package org.apache.flink.book.ml

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

object ETLLoadData {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
        // 加载测试数据
        val survival = env.readCsvFile[(String, String, String, String)]("src/main/resources/haberman.data")
        val survivalLV =
          survival
          .map { tuple =>
            val list = tuple.productIterator.toList
            val numList = list.map(_.asInstanceOf[String].toDouble)
            LabeledVector(numList(3), DenseVector(numList.take(3).toArray))
          }
  }
}
