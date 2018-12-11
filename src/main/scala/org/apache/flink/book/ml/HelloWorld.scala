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

import java.io.File

import org.apache.flink.core.fs.FileSystem
import org.apache.flink.api.scala._
import org.apache.flink.book.utils.CommonUtils
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.MLUtils
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.math.Vector

object HelloWorld {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 加载训练集合
    val trainLibSVM: DataSet[LabeledVector] = MLUtils.readLibSVM(env, "src/main/resources/svmguide1.data")
    // 加载测试集合
    val testLibSVM: DataSet[LabeledVector] = MLUtils.readLibSVM(env, "src/main/resources/svmguide1.t")

    def normalizer: LabeledVector => LabeledVector = {
      lv =>
        LabeledVector(if (lv.label > 0.0) {
          1.0
        } else {
          -1.0
        }, lv.vector)
    }

    val astroTrain: DataSet[LabeledVector] = trainLibSVM.map(normalizer)
    val astroTest: DataSet[(Vector, Double)] =
      testLibSVM.map(normalizer).map(x => (x.vector, x.label))

    val svm = SVM() // 创建学习者
      .setBlocks(env.getParallelism)// 数据分块
      .setIterations(100) // 进行100次迭代
      .setRegularization(0.001) // 定义SVM算法的正则化常数。 值越高，权重向量的2-norm越小。
      .setStepsize(0.1) //定义权重向量更新的初始步长。 步长越大，权重向量更新对下一个权重向量值的贡献越大。 如果算法变得不稳定，则必须调整该值。 （默认值：1.0）
      .setSeed(42) // 初始化随机生成数种子

    // 模型学习
    svm.fit(astroTrain)

    //通过计算预测值并返回一对真实标签值和预测值来评估测试数据。 重要的是，实现选择一个Testing类型，从中可以提取真正的标签值。
    val evaluationPairs: DataSet[(Double, Double)] = svm.evaluate(astroTest)
    val tempFile = File.createTempFile("flink_ml", "tmp").getAbsolutePath
    evaluationPairs.writeAsCsv(tempFile, CommonUtils.line, " ", FileSystem.WriteMode.OVERWRITE)
    println("print the result by run cmd [ cat "+ tempFile + " ]" )

    env.execute()

  }

}
