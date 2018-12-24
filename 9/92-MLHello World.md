# 9.2 Apache Flink ML HelloWorld

## HelloWorld

### ML依赖 

我还是直观的以代码的方式开始ML的学习之旅，首先是搭建环境。如果您是已经学习过第三章的Fink Helloworld小节，那么您已经具备了基本的Flink开发环境，进行ML的学习，只需要在Flink开发环境的基础上添加如下maven依赖：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-ml_2.11</artifactId>
  <version>1.7.0</version>
</dependency>
```

### 数据加载

要加载要与FlinkML一起使用的数据，我们可以使用Flink的ETL功能，或者使用格式化数据的专用函数，例如LibSVM格式。对于监督学习问题，通常使用LabeledVector类来表示（标签，功能）示例。 LabeledVector对象将具有表示示例的功能的FlinkML Vector成员和表示标签的Double成员，该成员可以是分类问题中的类，或者是回归问题的因变量。

例如，我们可以使用Haberman的生存数据集，对应源代码工程的rhaberman.data文件。该数据集“包含对接受过乳腺癌手术的患者的生存进行的研究的病例”。数据以逗号分隔的文件形式出现，其中前3列是特征，最后一列是类，第4列表示患者是否存活了5年或更长时间（标签1），或者是否在5年内死亡（标签） 2）我们创建HelloWorld类，并加载haberman.data数据，并将数据转换为DataSet [LabeledVector]如下:

```scala
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector

object ETLLoadData {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
        // 加载测试数据
        val survival = env.readCsvFile[(String, String, String, String)]("src/main/resources/haberman.data")
        
      // 构建我们需要的LabeledVector数据结构
      //数据集的第4个元素是类标签，其余的是要素，所以我们可以像这样构建LabeledVector元素：
      val survivalLV =
          survival
          .map { tuple =>
            val list = tuple.productIterator.toList
            val numList = list.map(_.asInstanceOf[String].toDouble)
            LabeledVector(numList(3), DenseVector(numList.take(3).toArray))
          }
  }
}
```

获得了上面的DataSet之后，我们就可以使用这些数据来培训学习者了。

### HelloWorld

 接下来我们将使用另一个数据集来例证构建学习者; 并且用LibSVM的方式创建学习者数据集。LibSVM是ML数据集的通用格式，可以在LibSVM数据集[网站](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/）中找到使用该格式的许多数据集。 FlinkML提供了通过MLUtils对象提供的readLibSVM函数使用LibSVM格式加载数据集的实用程序。 您还可以使用writeLibSVM函数以LibSVM格式保存数据集。 让我们导入svmguide1数据集,对应训练集合svmguide1.data和测试集合svmguide1.t 。这是一个astroparticle二元分类数据集。我们创建HelloWorld主类并加载数据，代码如下：

```scala
package org.apache.flink.book.ml

import org.apache.flink.api.scala._
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.MLUtils
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.math.Vector

object HelloWorld {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    // 加载训练集合
    val astroTrainLibSVM: DataSet[LabeledVector] = MLUtils.readLibSVM(env, "src/main/resources/svmguide1.data")
    // 加载测试集合
    val astroTestLibSVM: DataSet[LabeledVector] = MLUtils.readLibSVM(env, "src/main/resources/svmguide1.t")
  }

}

```

这里简单说明一下libsvm的数据格式，libsvm使用的训练数据和检验数据文件格式如下：

```scala
[label] [index1]:[value1] [index2]:[value2] …
[label] [index1]:[value1] [index2]:[value2] …
```

- label  - 目标值，就是说class（属于哪一类），就是你要分类的种类，通常是一些整数。
- index -  是有顺序的索引，通常是连续的整数。就是指特征编号，必须按照升序排列
- value - 就是特征值，用来train的数据，通常是一堆实数组成。

也就是

```
目标值 第一维特征编号：第一维特征值   第二维特征编号：第二维特征值 …
目标值 第一维特征编号：第一维特征值   第二维特征编号：第二维特征值 …
```

我们看一下上面我们加载的train数据片段：

```
1 1:2.617300e+01 2:5.886700e+01 3:-1.894697e-01 4:1.251225e+02
```

表示训练特征有4维，第一维是2.617300e+01，第二维是5.886700e+01，第三维是-1.894697e-01，第四维是1.251225e+0  目标值是1 。

好当我们了解了基本的数据结构之后接下来我们利用上面的训练集合和测试集合继续一个简单的分类学习(Classification)。导入训练和测试数据集后，需要为分类做好准备。 由于Flink SVM仅支持+1.0和-1.0的阈值二进制值，因此在加载LibSVM数据集后需要进行转换，因为它使用1和0进行标记。可以使用简单的规范化器映射函数完成转换：

```scala
def normalizer : LabeledVector => LabeledVector = { 
    lv => LabeledVector(if (lv.label > 0.0) 1.0 else -1.0, lv.vector)
}
val astroTrain: DataSet[LabeledVector] = astroTrainLibSVM.map(normalizer)
val astroTest: DataSet[(Vector, Double)] = astroTestLibSVM.map(normalizer).map(x => (x.vector, x.label))
```

现在我们转换了数据集，就可以训练预测器，例如线性SVM分类器。 我们可以为分类器设置许多参数。 这里我们设置Blocks参数，用于通过底层CoCoA算法使用来分割输入。正则化参数确定所应用的l2正则化的量，其用于避免过度拟合。 步长确定权重向量更新对下一个权重向量值的贡献。 此参数设置初始步长。

```scala
val svm = SVM()
  .setBlocks(env.getParallelism)
  .setIterations(100)// 进行100次迭代
  .setRegularization(0.001)
  .setStepsize(0.1)// 设置步长
  .setSeed(42)

svm.fit(astroTrain)
```

最后的完整代码如下：

```scala
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

```

 上面代码不用有特别细致的理解，后面会有SVM的小结进行详细介绍。