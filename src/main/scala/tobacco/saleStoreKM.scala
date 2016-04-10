package tobacco

import data.copy._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import KM_classic.{saleparser,storeparser,Retailerparser}

/**
  * Created by xcheng on 4/8/16.
  */
object saleStoreKM {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("店铺聚类").setMaster("local[4]")
    val sparkContext: SparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("OFF")
    val sqlContext: SQLContext = new SQLContext(sparkContext)

    val SalePath="/Users/xcheng/Desktop/实验室/销售库存/saledata/*"
    val StorePath ="/Users/xcheng/Desktop/实验室/销售库存/storedata/*"
    val RetailerPath="/Users/xcheng/360云盘/零售户2013.csv"
/*
  get sale and store table
 */
    val rawsale: RDD[String] = sparkContext.textFile(SalePath)
    val parsedsale: RDD[(String, String, String,String,String)] = rawsale.filter(!isColumnNameLine(_,"SALE_ID")).map(line=>saleparser(line))
    sqlContext.createDataFrame(parsedsale).toDF("ITEM_CODE","QTY","COM_NAME","PRICE","AMT").registerTempTable("saletable")
    val rawStore: RDD[String] = sparkContext.textFile(StorePath)
    val parsedStore: RDD[(Int, Int, Int, String)] = rawStore.filter(!isColumnNameLine(_,"COM_NAME")).map(line=>storeparser(line))
    sqlContext.createDataFrame(parsedStore).toDF("ITEM_CODE","QTY","DATE","COM_NAME").registerTempTable("storetable")
    val rawRetailer =sparkContext.textFile(RetailerPath)
    val parsedRetailer= rawRetailer.filter(!isColumnNameLine(_,"卷烟编码")).map(line=>Retailerparser(line))
    sqlContext.createDataFrame(parsedRetailer).toDF("DAY","ORG_CODE","ORG_NAME","TOBACCO_CODE","TOBACCO_NAME" , "wholesalePrice", "CUS_CODE", "CUS_NAME",
      "ADDRESS", "STATUS", "RetailStatu", "PAYKIND" , "CUSFLOW"
      , "MARKETTYPE", "NEED", "SHENHE", "DINGGOU", "PRICETYPE", "TAR_CONT", "AMOUNT", "MINUTE").registerTempTable("Retailertable")

    /*
    对Retailertable进行分析
     */






   /*
   get ALL saledata and ALL storedata per com
    */
/*    sqlContext.sql("select COM_NAME, sum(QTY)sumQTY ,sum(AMT)sumAMT FROM saletable group by COM_NAME").registerTempTable("totalsale")

    sqlContext.sql("select COM_NAME,sumAMT/sumQTY avgprice from totalsale ").registerTempTable("comavgprice")//.show()

    sqlContext.sql("select COM_NAME,sum(QTY)sumstore,FROM storetable group by COM_NAME").registerTempTable("totalstore")

    sqlContext.sql("select COM_NAME,sum")

    val qtyprice= sqlContext.sql("select a.COM_NAME,sumQTY,avgprice from totalsale a join comavgprice b on a.COM_NAME=b.COM_NAME")

    val tranningdata = qtyprice.map{
      case Row(com_name,sumQTY,avgprice)=>
        val features =Array[Double](sumQTY.toString.toDouble,avgprice.toString.toDouble)
        Vectors.dense(features)
    }.cache()

    /*
 特征标准化
  */  */

 /*   val scaler = new StandardScaler(
      withMean = true,withStd = true
    ).fit(tranningdata)
    val scaledVectors =tranningdata.map(v => scaler.transform(v))
    println("data after scale:")
    // scaledVectors.take(10).foreach(println)
    print(scaledVectors.count())

    val numClusters = 3*/

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
   /* val model = KMeans.train(scaledVectors,numClusters,maxIterations = 15,runs=5)
    qtyprice.map{
      case Row(com_name,sumQTY,avgprice)=>
        val features =Array[Double](sumQTY.toString.toDouble,avgprice.toString.toDouble)
        val line= Vectors.dense(features)
        val scaledline=scaler.transform(line)
        val prediction = model.predict(scaledline)
        com_name+","+sumQTY+","+avgprice+","+scaledline(0)+","+scaledline(1)+","+prediction

    }.saveAsTextFile("/Users/xcheng/Spark/sparkout/totaldataKMtestall1")*/

    //    scaledVectors.map{
    //      case Row(sumQTY,avgprice)=>
    //       // val prediction = model.predict(Vectors.dense(Array[Double](sumQTY.toString.toDouble,avgprice.toString.toDouble)))
    //        val features = Array(sumQTY.toString.toDouble,avgprice.toString.toDouble)
    //        val line= Vectors.dense(features)
    //        val prediction = model.predict(line)
    //        sumQTY+" "+avgprice+" "+prediction
    //    }.saveAsTextFile("/Users/xcheng/Spark/sparkout/totaldataKMscaledpre")




  }

}
//   sqlContext.sql("select COM_NAME,ITEM_CODE,sum(QTY) ALLQTY from saletable group by ITEM_CODE,COM_NAME").registerTempTable("comitemqtytable")
//    sqlContext.sql("select count(distinct ITEM_CODE) from saletable").show()//386
//    sqlContext.sql("select count(distinct COM_NAME) FROM saletable").show()//76
//    sqlContext.sql("select count(*) from comitemqtytable").show()//5371
//  sqlContext.sql("select * from comitemqtytable order by COM_NAME,ITEM_CODE").show(5371)


/*
    sqlContext.sql("select * from Retailertable").show()
    sqlContext.sql("select TOBACCO_CODE,AMOUNT,MINUTE   from Retailertable").show()*/
// sqlContext.sql("select distinct(DAY) from Retailertable").show()//2013
//sqlContext.sql("select count(*) from Retailertable").show()//7476966
//sqlContext.sql("select distinct(ORG_NAME) from Retailertable").show()
/*
|ORG_NAME|
+--------+
| 贵阳市烟草公司|
|黔西南州烟草公司|
|六盘水市烟草公司|
| 遵义市烟草公司|
|黔东南州烟草公司|
| 安顺市烟草公司|
| 黔南州烟草公司|
| 铜仁市烟草公司|
| 毕节市烟草公司|
 */
/*  sqlContext.sql("select count(distinct(TOBACCO_CODE)) from Retailertable").show()//卷烟品牌数347

  sqlContext.sql("select count(distinct(ADDRESS)) from Retailertable").show()//销售地址数 122716

  sqlContext.sql("select count(distinct(CUS_CODE)) from Retailertable").show()//客户数183290

  sqlContext.sql("select * from Retailertable where SHENHE <> DINGGOU").show()//2367*/

/*
System.out.println("经营业态")
sqlContext.sql("select STATUS,sum(AMOUNT)SUMOUNT from Retailertable GROUP BY STATUS").foreach(println)
System.out.println("零售户状态")
sqlContext.sql("select RetailStatu,sum(AMOUNT)SUMOUNT from Retailertable GROUP BY RetailStatu").foreach(println)
System.out.println("客流量")
sqlContext.sql("select CUSFLOW,sum(AMOUNT)SUMOUNT from Retailertable GROUP BY CUSFLOW").foreach(println)
System.out.println("市场类型")
sqlContext.sql("select MARKETTYPE,sum(AMOUNT)SUMOUNT from Retailertable GROUP BY MARKETTYPE").foreach(println)
sqlContext.sql("select PAYKIND,sum(AMOUNT)SUMOUNT from Retailertable GROUP BY PAYKIND").foreach(println)*/
