package data

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by xcheng on 3/28/16.
  */
object CumCancellRate {

  //case class CANCELL(ORDERID:Int,DUTY:String)

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("CumCancellRate").setMaster("local[2]")
    val sparkContext: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sparkContext)




    ////获得REFUNDDELIVERYDUTY=CUM的记录的ORDERID第四个字段
    val rawCancellationOrder: RDD[String] = sparkContext.textFile("/Users/xcheng/Desktop/buybox/Cancellation_order.csv")


    //打印垃圾数据
//val sp = rawCancellationOrder.map(line=>line.split(",")).filter(_.length<32)//.filter(_(3)=="C")
//    sp.first().foreach(println)


//    //Order table
    val rawOrder: RDD[String] = sparkContext.textFile("/Users/xcheng/Desktop/buybox/Order.csv")

    //    orderdity.first().foreach(print )
//     //rawOrder.filter(_.split(",").length<100).take(1)
//    rawOrder.map(line=>{
//      val fields = line.split(",")
//      if (fields.length < 100) line
//      else null
//    }).filter(_ != null).collect().foreach(println)


    val parsedOrder: RDD[(Int,Int, String, String, Double, Int, Long)] = rawOrder.filter(!isColumnNameLine(_,"ORDERID")).filter(_.split(",").length==100).map(line=>Orderparser(line))
    sqlContext.createDataFrame(parsedOrder).toDF("SALE_DAY","ORDERID","KIND","SALE_TYPE","SALE_PRICE","SALE_AMT","VENDORITEMID").registerTempTable("parsedOrder")

    val parsedCancell: RDD[(Int, String)] = rawCancellationOrder.filter(!isColumnNameLine(_,"ORDERID")).filter(_.split(",").length==32).map(line=>Cancellpaser(line))
    sqlContext.createDataFrame(parsedCancell).toDF("ORDERID","DUTY").registerTempTable("parsedCancell")
    //Fulldata
    sqlContext.sql("select * from (select * from  parsedOrder a left join parsedCancell b on a.ORDERID=b.ORDERID )tmp ").registerTempTable("Fulldata")

    val ComCancellNum = sqlContext.sql("select VENDORITEMID,count(*) cancellnum from Fulldata where DUTY='COM' group by VENDORITEMID ")//VENDORITEMID订单取消数
    ComCancellNum.registerTempTable("ComCancellNum")

    //VENDOR取消率

    sqlContext.sql("select tmp.VENDORITEMID,cancellnum/ordernum cancellrate from (select VENDORITEMID,count(*) ordernum from Fulldata group by VENDORITEMID)tmp " +
      "inner join  ComCancellNum on tmp.VENDORITEMID=ComCancellNum.VENDORITEMID").registerTempTable("VENDORCANCELLRATE")


    /*
    * 线性回归: cancellrate,SALE_PRICE,-->SALE_AMT 一个SELLER IN ONE DAY 算一个样本
    * */
    val data = sqlContext.sql("select cancellrate,avg(SALE_PRICE) avg_price,sum(SALE_AMT) amt from parsedOrder a  left JOIN " +
      "VENDORCANCELLRATE b ON a.VENDORITEMID=b.VENDORITEMID group by SALE_DAY,a.VENDORITEMID,cancellrate")//非聚合函数必须出现在group by 里
    data.show()
    val trainingData = data.map { row =>
      val features = Array[Double](row(0).toString.toDouble, row(1).toString.toDouble)//, row(2).toString.toDouble)
      LabeledPoint(row(2).toString.toDouble, Vectors.dense(features))
    }


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
