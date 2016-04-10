package data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.StandardScaler

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xcheng on 3/17/16.
  * 实现对两个csv文件的outer join后的数据进行线性回归和逻辑回归
  */

object LogistRandLineR {
  case class Order(ORDERID:Int,SALE_PRICE:Double,SALE_AMT:Int,COUPANG_PRICE:Double)

  case class Cancell(ORDERID:Int,buy:Int)

  def main(args: Array[String]) {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("datamerge")//.setMaster("local[4]")
    val sc = new SparkContext(conf)
    import org.apache.spark.sql.SQLContext
    val sqlContext = new SQLContext(sc)



        //过滤首行列名
        def isColumnNameLine(line:String):Boolean = {
          if (line != null && line.contains("ORDERID"))true
          else false
        }
    //数据准备
      //(1)Order
    val rawOrder=sc.textFile("/Users/xcheng/Desktop/buybox/Order.csv")
    //  print (rawTrainningData.count())//9381001条
      import sqlContext.implicits._
    val parsedOrder = rawOrder.filter(!isColumnNameLine(_))
      .map(line=>{
        val splits=line.replace("\"","").split(",")//.++(1.toString)//.slice(1,4)//.foreach(_.subSequence(1,6))
        val index=Array(5,6,7,25)//,splits.length-1) //取指定字段SALE_BASIS_DY"20160120,"ORDERID","SALE_PRICE","SALE_AMT"
        index collect splits
      }).map(p=>Order(p(0).toInt ,p(1).toDouble,p(2).toInt,p(3).toDouble)).toDF("ORDERID","SALE_PRICE","SALE_AMT","COUPANG_PRICE")
    parsedOrder.registerTempTable("parsedOrdertable")

    parsedOrder.show(3)


    //(2)Cancellation_order.csv
    val Cancellation=sc.textFile("/Users/xcheng/Desktop/buybox/Cancellation_order.csv")
    val parsedCancellationOrder = Cancellation.filter(!isColumnNameLine(_)).filter(_.split(",").length==32)
      .map(line=>{
        val ORDERID=line.replace("\"","").split(",")(3)//.slice(1,4)//.foreach(_.subSequence(1,6))
//        val index=Array(3,23) //ORDERID,COMPLETEDDTTM  20150820020100
//        index collect splits
        ORDERID
      }).toDF("ORDERID")//.map(p=>Cancell(p(0).toDouble,p(1).toInt))
    parsedCancellationOrder.registerTempTable("cancelledtable")

    parsedCancellationOrder.show(3)


    //Order 与 Cancellation join


    sqlContext.sql("select SALE_PRICE,SALE_AMT , b.cancell from (select ORDERID,COUPANG_PRICE,SALE_PRICE,SALE_AMT from parsedOrdertable)a left outer join (" +
      "select ORDERID,1 as cancell from cancelledtable)b on a.ORDERID =b.ORDERID").registerTempTable("data")
    import sqlContext._
    val data =sql("select SALE_PRICE,SALE_AMT,IF(cancell is null, 0,cancell )AS CANCELL from  data")
        data.show()
    val dataPoints = data.map(r=>{
          val features = Vectors.dense(r(0).toString.toDouble,r(1).toString.toDouble)
          LabeledPoint(r(2).toString.toDouble, features)
        })

    System.out.print("特征向量-----------------------------")
    dataPoints.take(10).foreach(print)

//
//         val dataPoints = data.map(row=>new  LabeledPoint(row.getDouble(1),row.asInstanceOf))
//
//
//
//
//
//    //构造LabeledPoint
//    //val dataPoints = data.map(row=>new LabeledPoint(row.last.toDouble,Vectors.dense(row.take(row.length-1).map(str=>str.toDouble))))
//
    val dataSplit =  dataPoints.randomSplit(Array(0.8,0.2))
    val trainingSet = dataSplit(0)
    val testSet =  dataSplit(1)

    //特征标准化
    val scaler = new StandardScaler(withMean = true,withStd = true).fit(trainingSet.map(point => point.features))
    val scaledTrainingSet = trainingSet.map(point =>new LabeledPoint(point.label,scaler.transform(point.features))).cache()
    System.out.print("标准化后的特征向量--------------------------")
    scaledTrainingSet.take(10).foreach(print)
    val scaledTestSet = testSet.map(point => new LabeledPoint(point.label,scaler.transform(point.features))).cache()

//    //(1)线性回归
//    val regression = new LinearRegressionWithSGD().setIntercept(true)
//    regression.optimizer.setNumIterations(1000).setStepSize(0.1)
//    val model = regression.run(scaledTrainingSet)
//    //预测
//    val predictions = model.predict(scaledTestSet.map(point=>point.features))
//    //评估模型
//    val actuals = scaledTestSet.map(_.label)
//    val predictionsAndActuals  =predictions.zip(actuals)
//    val sumSquaredErrors = predictionsAndActuals.map{case (pred,act)=>
//      println(s" act,pred and difference $act,$pred ${act-pred}")
//      math.pow(act-pred,2)
//    }.sum()
//    val meanSquaredError = sumSquaredErrors/scaledTestSet.count()
//
//    println(s"SSE is $sumSquaredErrors")
//    println(s"MSE is $meanSquaredError")

    //(2)逻辑回归
    val lrModel=LogisticRegressionWithSGD.train(scaledTrainingSet,10)
    val predictrueData=scaledTrainingSet.map{point=>
      if(lrModel.predict(point.features)==point.label) 1 else 0
    }.sum()
    val accuracy=predictrueData/data.count()
    println(accuracy)
  print("lrModel.weights="+lrModel.weights)
/*
0.5192469735549867
lrModel.weights=[-0.3450033514239469]
 */

  }

}

