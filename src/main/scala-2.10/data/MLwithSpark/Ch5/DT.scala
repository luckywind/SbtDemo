package data.MLwithSpark.Ch5

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.{Gini, Impurity, Entropy}

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.feature.StandardScaler

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.classification.ClassificationModel


/**
  * Created by xcheng on 4/5/16.
  */
object DT {
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new  SparkConf().setAppName("决策树").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rawData = sc.textFile("/Users/xcheng/Spark/pyspark/MLwithSpark/train_noheader.tsv")
    /*
    "http://www.menshealth.com/health/flu-fighting-fruits?cm_mmc=Facebook-_-MensHealth-_-Content-Health-_-F
    ightFluWithFruit"	"1164"	"{""title"":""Fruit}"	"health"

    	"0.996526"	"2.382882883"	"0.562015504"	"0.321705426"
    "0.120155039"	"0.042635659"	"0.525448029"	"0"	"0"	"0.072447859"
    	"0"	"0.22640177"	"0.120535714"	"1"	"1"	"55"	"0"	"2240"	"258"	"11"	"0.166666667"	"0.057613169"	"1"

     */
    val records = rawData.map(line => line.split("\t"))
    val data = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }
    data.cache
    val numData = data.count

    val numIterations = 10
    val maxTreeDepth = 5
    val dtModel = DecisionTree.train(data, Algo.Classification, Entropy, maxTreeDepth)

    //计算准确度decision tree threshold needs to be specified
    val dtTotalCorrect = data.map { point =>
      val score = dtModel.predict(point.features)
      val predicted = if (score > 0.5) 1 else 0
      if (predicted == point.label) 1 else 0
    }.sum
    val dtAccuracy = dtTotalCorrect / numData
    // dtAccuracy: Double = 0.6482758620689655

    val dtMetrics = Seq(dtModel).map{ model =>
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (model.getClass.getSimpleName, metrics.areaUnderPR, metrics.areaUnderROC)
    }


    /**
    考虑添加类别特征
      */
    // Investigate the impact of adding in the 'category' feature
    val categories = records.map(r => r(3)).distinct.collect.zipWithIndex.toMap
    // categories: scala.collection.immutable.Map[String,Int] = Map("weather" -> 0, "sports" -> 6,
    //	"unknown" -> 4, "computer_internet" -> 12, "?" -> 11, "culture_politics" -> 3, "religion" -> 8,
    // "recreation" -> 2, "arts_entertainment" -> 9, "health" -> 5, "law_crime" -> 10, "gaming" -> 13,
    // "business" -> 1, "science_technology" -> 7)
    val numCategories = categories.size
    // numCategories: Int = 14
    val dataCategories = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt

      val categoryIdx = categories(r(3))
      val categoryFeatures = Array.ofDim[Double](numCategories)
      categoryFeatures(categoryIdx) = 1.0

      val otherFeatures = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      val features = categoryFeatures ++ otherFeatures
      LabeledPoint(label, Vectors.dense(features))
    }

    // standardize the feature vectors
    val scalerCats = new StandardScaler(withMean = true, withStd = true).fit(dataCategories.map(lp => lp.features))
    val scaledDataCats = dataCategories.map(lp => LabeledPoint(lp.label, scalerCats.transform(lp.features)))

    // investigate tree depth impact for Entropy impurity
    val dtResultsEntropy = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
      val model = trainDTWithParams(data, param, Entropy)
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param tree depth", metrics.areaUnderROC)
    }
    dtResultsEntropy.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
    /*
    1 tree depth, AUC = 59.33%
    2 tree depth, AUC = 61.68%
    3 tree depth, AUC = 62.61%
    4 tree depth, AUC = 63.63%
    5 tree depth, AUC = 64.88%
    10 tree depth, AUC = 76.26%
    20 tree depth, AUC = 98.45%
    */

    // investigate tree depth impact for Gini impurity
    val dtResultsGini = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
      val model = trainDTWithParams(data, param, Gini)
      val scoreAndLabels = data.map { point =>
        val score = model.predict(point.features)
        (if (score > 0.5) 1.0 else 0.0, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param tree depth", metrics.areaUnderROC)
    }
    dtResultsGini.foreach { case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
    /*
    1 tree depth, AUC = 59.33%
    2 tree depth, AUC = 61.68%
    3 tree depth, AUC = 62.61%
    4 tree depth, AUC = 63.63%
    5 tree depth, AUC = 64.89%
    10 tree depth, AUC = 78.37%
    20 tree depth, AUC = 98.87%
    */





  }

  // helper function to train a logistic regresson model
  def trainWithParams(input: RDD[LabeledPoint], regParam: Double, numIterations: Int, updater: Updater, stepSize: Double) = {
    val lr = new LogisticRegressionWithSGD
    lr.optimizer.setNumIterations(numIterations).setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
    lr.run(input)}
  // helper function to create AUC metric
  def createMetrics(label: String, data: RDD[LabeledPoint], model: ClassificationModel) = {
    val scoreAndLabels = data.map { point =>
      (model.predict(point.features), point.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    (label, metrics.areaUnderROC)
  }

  def trainDTWithParams(input: RDD[LabeledPoint], maxDepth: Int, impurity: Impurity) = {
    DecisionTree.train(input, Algo.Classification, impurity, maxDepth)
  }

}
