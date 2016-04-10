package tobacco

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import data.copy.{getspkCtxAndsqlCtx,isColumnNameLine}


/**
  * Created by xcheng on 4/6/16.
  *
  * 门店销量,平均价格聚类
  *
  */
object KM_classic {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("店铺聚类").setMaster("local[4]")
    val sparkContext: SparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("OFF")
    val sqlContext: SQLContext = new SQLContext(sparkContext)


   // val (sparkContext: SparkContext, sqlContext: SQLContext) = getspkCtxAndsqlCtx("店铺聚类")
    val SalePath="/Users/xcheng/Desktop/实验室/销售库存/saledata/*"

    val rawsale: RDD[String] = sparkContext.textFile(SalePath)
    val parsedsale: RDD[(String, String, String,String,String)] = rawsale.filter(!isColumnNameLine(_,"SALE_ID")).map(line=>saleparser(line))
    sqlContext.createDataFrame(parsedsale).toDF("ITEM_CODE","QTY","COM_NAME","PRICE","AMT").registerTempTable("saletable")
//   sqlContext.sql("select COM_NAME,ITEM_CODE,sum(QTY) ALLQTY from saletable group by ITEM_CODE,COM_NAME").registerTempTable("comitemqtytable")
//    sqlContext.sql("select count(distinct ITEM_CODE) from saletable").show()//386
//    sqlContext.sql("select count(distinct COM_NAME) FROM saletable").show()//76
//    sqlContext.sql("select count(*) from comitemqtytable").show()//5371
  //  sqlContext.sql("select * from comitemqtytable order by COM_NAME,ITEM_CODE").show(5371)
    sqlContext.sql("select COM_NAME, sum(QTY)sumQTY ,sum(AMT)sumAMT FROM saletable group by COM_NAME").registerTempTable("totalsale")

  sqlContext.sql("select COM_NAME,sumAMT/sumQTY avgprice from totalsale ").registerTempTable("comavgprice")//.show()

   val qtyprice= sqlContext.sql("select a.COM_NAME,sumQTY,avgprice from totalsale a join comavgprice b on a.COM_NAME=b.COM_NAME")

    val tranningdata = qtyprice.map{
      case Row(com_name,sumQTY,avgprice)=>
        val features =Array[Double](sumQTY.toString.toDouble,avgprice.toString.toDouble)
        Vectors.dense(features)
    }.cache()

    /*
 特征标准化
  */
    val scaler = new StandardScaler(
      withMean = true,withStd = true
    ).fit(tranningdata)
    val scaledVectors =tranningdata.map(v => scaler.transform(v))
    println("data after scale:")
   // scaledVectors.take(10).foreach(println)
     print(scaledVectors.count())

    val numClusters = 3

//    val numIterations = Array(5,10,15,20,30,50,100)
//    numIterations.foreach{it=>
//      val kmeansplusmodel = KMeans.train(scaledVectors,numClusters,it)
//      /*
//      计算组内误差
//       */
//      val cost=kmeansplusmodel.computeCost(scaledVectors)
//      println("kmeans++ "+"iter="+it+" Within Set Sum of Squared Errors="+cost)
//
//    }
//    val runs = Array(5,10,15,20,30,50)
//    runs.foreach{r=>
//      val kmeansplusmodel = KMeans.train(scaledVectors,numClusters,maxIterations = 5,runs=r)
//      val cost=kmeansplusmodel.computeCost(scaledVectors)
//      println("kmeans++ "+"runs="+r+"Within Set Sum of Squared Errors="+cost)
//
//      val kmeanmodel = KMeans.train(scaledVectors,numClusters,maxIterations = 5,runs=r,initializationMode = "random")
//      val kmeanscost=kmeanmodel.computeCost(scaledVectors)
//      println("kmeans                               "+"runs="+r+"Within Set Sum of Squared Errors="+kmeanscost)
//    }
//
//    val K =Array(2,3,4,5,6,7,8,9)
//    K.foreach{k=>
//      val model = KMeans.train(scaledVectors,k,maxIterations = 5,runs=5)
//      /*
//      计算组内误差
//       */
//      val cost=model.computeCost(scaledVectors)
//      println("kmeans++ "+"k="+k+"Within Set Sum of Squared Errors="+cost)
//    }

    /*
    选出的模型
     */
       val model = KMeans.train(scaledVectors,numClusters,maxIterations = 15,runs=5)
    qtyprice.map{
      case Row(com_name,sumQTY,avgprice)=>
        val features =Array[Double](sumQTY.toString.toDouble,avgprice.toString.toDouble)
        val line= Vectors.dense(features)
        val scaledline=scaler.transform(line)
        val prediction = model.predict(scaledline)
        com_name+","+sumQTY+","+avgprice+","+scaledline(0)+","+scaledline(1)+","+prediction

    }.saveAsTextFile("/Users/xcheng/Spark/sparkout/totaldataKMtestall1")

//    scaledVectors.map{
//      case Row(sumQTY,avgprice)=>
//       // val prediction = model.predict(Vectors.dense(Array[Double](sumQTY.toString.toDouble,avgprice.toString.toDouble)))
//        val features = Array(sumQTY.toString.toDouble,avgprice.toString.toDouble)
//        val line= Vectors.dense(features)
//        val prediction = model.predict(line)
//        sumQTY+" "+avgprice+" "+prediction
//    }.saveAsTextFile("/Users/xcheng/Spark/sparkout/totaldataKMscaledpre")






  }
  def saleparser(line:String)={
    val splited = line.split(",")
    val ITEM_CODE=splited(4)
    val QTY=splited(5)
    val COM_NAME=splited(8)
    val PRICE=splited(6)
    val AMT = splited(7)
    (ITEM_CODE,QTY,COM_NAME,PRICE,AMT)
  }
  def storeparser(line:String)={
    val splited = line.split(",")   //LICENSE_CODE,ITEM_CODE,QTY,DATE1,TIME1,COM_NAME
    val ITEM_CODE =splited(1).toInt
    val QTY=splited(2).toInt
    val DATE=splited(3).toInt
    val COM_NAME = splited(5).toString
    (ITEM_CODE,QTY,DATE,COM_NAME)

  }
  def Retailerparser(line:String)={
    val splited = line.split(",")
    val DAY = splited(0)
    val ORG_CODE=splited(1)
    val ORG_NAME=splited(2)
    val TOBACCO_CODE = splited(3)
    val TOBACCO_NAME = splited(4)
    val wholesalePrice = splited(5)
    val CUS_CODE = splited(6)
    val CUS_NAME=splited(7)
    val ADDRESS=splited(8)
    val STATUS = splited(9)
    val RetailStatu =splited(10)
    val PAYKIND=splited(11)
    val CUSFLOW= splited(12)
    val MARKETTYPE=splited(13)
    val NEED=splited(14)
    val SHENHE=splited(15)
    val DINGGOU=splited(16)
    val PRICETYPE=splited(17)
    val TAR_CONT=splited(18)
    val AMOUNT=splited(19)
    val MINUTE =splited(20)

    (DAY,ORG_CODE,ORG_NAME,TOBACCO_CODE,TOBACCO_NAME , wholesalePrice, CUS_CODE, CUS_NAME, ADDRESS, STATUS, RetailStatu, PAYKIND , CUSFLOW
    , MARKETTYPE, NEED, SHENHE, DINGGOU, PRICETYPE, TAR_CONT, AMOUNT, MINUTE)

  }


}
