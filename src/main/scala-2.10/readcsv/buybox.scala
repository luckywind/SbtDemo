package readcsv

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.feature.StandardScaler

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xcheng on 3/17/16.
  */

object buybox {
  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("buybox")//.setMaster("local[4]")
    val sc = new SparkContext(conf)

    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)

    //数据准备
    val rawOrder=sc.textFile("/Users/xcheng/Desktop/buybox/Order.csv")
    //  print (rawTrainningData.count())//9381001条
    //过滤首行列名
    def isColumnNameLine(line:String):Boolean = {
      if (line != null && line.contains("SALE_BASIS_DY"))true
      else false
    }
    val parsedOrder = rawOrder.filter(!isColumnNameLine(_))
      .map(line=>{
        val splits=line.replace("\"","").split(",")//.slice(1,4)//.foreach(_.subSequence(1,6))
        val index=Array(0,1,2,4,6,5) //取指定字段
        index collect splits
      })
      parsedOrder.first().foreach(println)
    import sqlContext.implicits._

    //构造LabeledPoint
    val dataPoints = parsedOrder.map(row=>new LabeledPoint(row.last.toDouble,Vectors.dense(row.take(row.length-1).map(str=>str.toDouble))))

      val dataSplit =  dataPoints.randomSplit(Array(0.8,0.2))
    val trainingSet = dataSplit(0)
    val testSet =  dataSplit(1)

    //特征标准化
    val scaler = new StandardScaler(withMean = true,withStd = true).fit(trainingSet.map(point => point.features))
    val scaledTrainingSet = trainingSet.map(point =>new LabeledPoint(point.label,scaler.transform(point.features))).cache()
    val scaledTestSet = testSet.map(point => new LabeledPoint(point.label,scaler.transform(point.features))).cache()

    //线性回归
    val regression = new LinearRegressionWithSGD().setIntercept(true)
    regression.optimizer.setNumIterations(1000).setStepSize(0.1)
    val model = regression.run(scaledTrainingSet)
    //预测
    val predictions = model.predict(scaledTestSet.map(point=>point.features))
    //评估模型
    val actuals = scaledTestSet.map(_.label)
    val predictionsAndActuals  =predictions.zip(actuals)
    val sumSquaredErrors = predictionsAndActuals.map{case (pred,act)=>
    println(s" act,pred and difference $act,$pred ${act-pred}")
    math.pow(act-pred,2)
    }.sum()
    val meanSquaredError = sumSquaredErrors/scaledTestSet.count()

    println(s"SSE is $sumSquaredErrors")
    println(s"MSE is $meanSquaredError")



  }

}

/*
sbt compile
sbt package
//run
/Users/xcheng/Spark/ideaProj/SbtDemo/target/scala-2.10
Admins-MacBook-Pro-24:scala-2.10 xcheng$ spark-submit --master spark://localhost:7077 --class readcsv.buybox
 --executor-memory 3g --total-executor-cores 4 sbtdemo_2.10-1.0.jar
 //result
SSE is 4.446793439740165E21
MSE is 2.3713803385147575E15
 */

