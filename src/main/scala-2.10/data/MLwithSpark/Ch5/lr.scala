package data.MLwithSpark.Ch5

import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.feature.StandardScaler

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.optimization.SquaredL2Updater

/**
  * Created by xcheng on 4/5/16.
  */
object lr {
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setAppName("LogisticRegretion").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rawData = sc.textFile("/Users/xcheng/Spark/pyspark/MLwithSpark/train_noheader.tsv")
    val records = rawData.map(line => line.split("\t"))

    val data = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }
    data.cache
    val numData = data.count

    // train a Logistic Regression model
    val numIterations = 10
    val maxTreeDepth = 5
    val lrModel = LogisticRegressionWithSGD.train(data, numIterations)//.clearThreshold()返回原始得分//.setThreshold(0.5)
       // new LogisticRegressionWithLBFGS  效果更好,类似于牛顿法,比SGD迭代次数更少

    // compute accuracy for logistic regression
    val lrTotalCorrect = data.map { point =>
      if (lrModel.predict(point.features) == point.label) 1 else 0
    }.sum
    // lrTotalCorrect: Double = 3806.0

    // accuracy is the number of correctly classified examples (same as true label)
    // divided by the total number of examples
    val lrAccuracy = lrTotalCorrect / numData





  }

}
