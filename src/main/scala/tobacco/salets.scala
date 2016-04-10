package tobacco

import data.copy._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}


/**
  * Created by xcheng on 4/6/16.
  */
object salets {
  def main(args: Array[String]) {
    //val (sparkContext: SparkContext, sqlContext: SQLContext) = getspkCtxAndsqlCtx("销售时间序列分析")
    val conf: SparkConf = new SparkConf().setAppName("销售时间序列").setMaster("local[4]")
    val sparkContext: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sparkContext)
    //val SalePath="/Users/xcheng/Desktop/实验室/销售库存/红华销售数据002.csv"
    val SalePath= "/Users/xcheng/Desktop/实验室/销售库存/saledata/*"
    val rawsale: RDD[String] = sparkContext.textFile(SalePath)
    val parsedsale: RDD[(Long,String, Double, String,String,String)] = rawsale.filter(!isColumnNameLine(_,"SALE_ID")).map(line=>saleparser(line))
    sqlContext.createDataFrame(parsedsale).toDF("PUH_TIME","ITEM_CODE","QTY","COM_NAME","PRICE","AMT").registerTempTable("saletable")

    sqlContext.sql("select COM_NAME, sum(QTY)sumQTY ,sum(AMT)sumAMT FROM saletable group by COM_NAME").registerTempTable("totalsale")

    sqlContext.sql("select  PUH_TIME ,sum(QTY) saleQTY FROM saletable group by PUH_TIME order by PUH_TIME").orderBy("PUH_TIME").foreach(println )





  }
  def saleparser(line:String)={
    val splited = line.split(",")
    val PUH_TIME ={val time=splited(3).trim.toLong/1000000;if(time>30000000)time/10 else time}
    val ITEM_CODE=splited(4)
    val QTY=splited(5).toDouble
    val COM_NAME=splited(8)
    val PRICE=splited(6)
    val AMT = splited(7)
    (PUH_TIME,ITEM_CODE,QTY,COM_NAME,PRICE,AMT)
  }


}

/*
 //sqlContext.sql("select COM_NAME,ITEM_CODE,sum(QTY) ALLQTY from saletable group by ITEM_CODE,COM_NAME").registerTempTable("comitemqtytable")
    // sqlContext.sql("select count(distinct ITEM_CODE) from saletable").show()//114
    //    sqlContext.sql("select count(distinct COM_NAME) FROM saletable").show()//76
    //    sqlContext.sql("select count(*) from comitemqtytable").show()//5371
    //    sqlContext.sql("select * from comitemqtytable order by COM_NAME,ITEM_CODE").show(5371)
 */