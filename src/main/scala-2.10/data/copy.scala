package data

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xcheng on 3/28/16.
  */
object copy {

  //case class CANCELL(ORDERID:Int,DUTY:String)

  def main(args: Array[String]) {
    val Appname="CumCancellRate"
    val (sparkContext: SparkContext, sqlContext: SQLContext) = getspkCtxAndsqlCtx(Appname)



    createTable(sparkContext, sqlContext)


    /*
    * 线性回归: cancellrate,SALE_PRICE,-->SALE_AMT 一个SELLER IN ONE DAY 算一个样本
    * */
    val data = sqlContext.sql("select cancellrate,avg(SALE_PRICE) avg_price,sum(SALE_AMT) amt from parsedOrder a  left JOIN " +
      "VENDORCANCELLRATE b ON a.VENDORITEMID=b.VENDORITEMID where cancellrate is not null group by SALE_DAY,a.VENDORITEMID,cancellrate")//非聚合函数必须出现在group by 里
     data.show()
    /*
    * |         cancellrate|        avg_price|amt|
+--------------------+-----------------+---+
| 0.10526315789473684|           2950.0|  2|
| 0.10526315789473684|           2950.0|  1|
| 0.10526315789473684|983.3333333333334|  1|
| 0.10526315789473684|           2950.0|  1|
| 0.10526315789473684|              0.0|  0|
|                null|          13800.0|  1|
|                null|          19500.0|  2|
+--------------------+-----------------+---+
|         cancellrate|        avg_price|amt|
+--------------------+-----------------+---+
| 0.10526315789473684|           2950.0|  2|
| 0.10526315789473684|           2950.0|  1|
| 0.10526315789473684|983.3333333333334|  1|
| 0.10526315789473684|           2950.0|  1|
| 0.10526315789473684|              0.0|  0|
|0.007005253940455...|              0.0|  0|
|0.007005253940455...|6572.093023255814|157|
|0.007005253940455...|           9374.4|217|
| 0.18181818181818182|           6700.0|  1|
| 0.18181818181818182|           6700.0|  1|
| 0.18181818181818182|           6700.0|  2|
| 0.18181818181818182|              0.0|  0|
| 0.18181818181818182|           6700.0|  1|
| 0.18181818181818182|           6700.0|  1|
| 0.18181818181818182|              0.0|  0|
| 0.18181818181818182|           3350.0|  1|
| 0.18181818181818182|           6700.0|  1|*/

    val trainingData = data.map { row =>
      val features = Array[Double](row(0).toString.toDouble, row(1).toString.toDouble)//, row(2).toString.toDouble)
      LabeledPoint(row(2).toString.toDouble, Vectors.dense(features))
    }.cache()
   // trainingData.saveAsTextFile("/Users/xcheng/Spark/ideaProj/SbtDemo/target/scala-2.10/traningData")

        //(1)线性回归
        val regression = new LinearRegressionWithSGD().setIntercept(true)
        regression.optimizer.setNumIterations(1000).setStepSize(0.1)
        val model = regression.run(trainingData)
        print("线性回归权重="+model.weights)

//        //预测
//        val predictions = model.predict(trainingData.map(point=>point.features))
//        //评估模型
//        val actuals = trainingData.map(_.label)
//        val predictionsAndActuals  =predictions.zip(actuals)
//        val sumSquaredErrors = predictionsAndActuals.map{case (pred,act)=>
//          println(s" act,pred and difference $act,$pred ${act-pred}")
//          math.pow(act-pred,2)
//        }.sum()
//        val meanSquaredError = sumSquaredErrors/trainingData.count()
//
//        println(s"SSE is $sumSquaredErrors")
//        println(s"MSE is $meanSquaredError")






    //sqlContext.sql("select distinct(VENDORITEMID)  from parsedOrder ").show()

  }

  def createTable(sparkContext: SparkContext, sqlContext: SQLContext): Unit = {
    val OrderTablePath="/Users/xcheng/Desktop/buybox/Order.csv"
    val rawCancellationOrderPath="/Users/xcheng/Desktop/buybox/Cancellation_order.csv"

    val rawOrder: RDD[String] = sparkContext.textFile(OrderTablePath)
    val parsedOrder: RDD[(Int, Int, String, String, Double, Int, Long)] = rawOrder.filter(!isColumnNameLine(_, "ORDERID")).filter(_.split(",").length == 100).map(line => Orderparser(line))
    sqlContext.createDataFrame(parsedOrder).toDF("SALE_DAY", "ORDERID", "KIND", "SALE_TYPE", "SALE_PRICE", "SALE_AMT", "VENDORITEMID").registerTempTable("parsedOrder")

    ////获得REFUNDDELIVERYDUTY=CUM的记录的ORDERID第四个字段
    val rawCancellationOrder: RDD[String] = sparkContext.textFile(rawCancellationOrderPath)
    val parsedCancell: RDD[(Int, String)] = rawCancellationOrder.filter(!isColumnNameLine(_, "ORDERID")).filter(_.split(",").length == 32).map(line => Cancellpaser(line))
    sqlContext.createDataFrame(parsedCancell).toDF("ORDERID", "DUTY").registerTempTable("parsedCancell")
    //Fulldata
    sqlContext.sql("select * from (select * from  parsedOrder a left join parsedCancell b on a.ORDERID=b.ORDERID )tmp ").registerTempTable("Fulldata")

    val ComCancellNum = sqlContext.sql("select VENDORITEMID,count(*) cancellnum from Fulldata where DUTY='COM' group by VENDORITEMID ") //VENDORITEMID订单取消数
    ComCancellNum.registerTempTable("ComCancellNum")

    //VENDOR取消率

    sqlContext.sql("select tmp.VENDORITEMID,cancellnum/ordernum cancellrate from (select VENDORITEMID,count(*) ordernum from Fulldata group by VENDORITEMID)tmp " +
      "inner join  ComCancellNum on tmp.VENDORITEMID=ComCancellNum.VENDORITEMID").registerTempTable("VENDORCANCELLRATE")
  }

  def getspkCtxAndsqlCtx(Appname: String): (SparkContext, SQLContext) = {
    val conf: SparkConf = new SparkConf().setAppName(Appname).setMaster("local[2]")
    val sparkContext: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    (sparkContext, sqlContext)
  }


  //过滤首行列名
  def isColumnNameLine(line:String,columnname:String):Boolean = {
    if (line != null && line.contains(columnname))true
    else false
  }

  def Cancellpaser(line:String) ={
    val splited = line.replace("\"","").split(",")



    val ORDERID = splited(3).toInt

    val DUTY   = splited(15)
    // CANCELL(ORDERID,DUTY)
    (ORDERID,DUTY)
  }

  def Orderparser(line:String) ={
    val splited = line.replace("\"","").split(",")
    val SALE_DAY=splited(0).toInt
    val KIND = splited(2)
    val SALE_TYPE =splited(3)
    val ORDERID = splited(5).toInt
    val SALE_PRICE   = splited(6).toDouble
    val SALE_AMT = splited(7).toInt
    val VENDORITEMID =if(splited(90)=="")0 else splited(90).toLong //toLong


    (SALE_DAY,ORDERID,KIND,SALE_TYPE,SALE_PRICE,SALE_AMT,VENDORITEMID)
  }

}
//delivery time ,price
//建 ncds
//提供数据格式给PAUL